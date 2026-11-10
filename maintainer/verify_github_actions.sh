#!/usr/bin/env bash
# Read-only. Requires Bash, curl, jq, git, awk, and GNU date.
# Usage: bash maintainer/verify_github_actions.sh OWNER/REPO [OWNER/REPO ...]
# Select the highest stable numeric release version published >= 7 days ago,
# across all majors. Ignore nonnumeric/prerelease tags; prefer more-specific
# tags on version ties, then the most recently published release.
# Repeated identical repository arguments are queried once per invocation.
set -euo pipefail

if (( $# == 0 )); then
    echo "Usage: bash $0 OWNER/REPO [OWNER/REPO ...]" >&2
    exit 2
fi
cutoff=$(date -u -d '7 days ago' '+%Y-%m-%dT%H:%M:%SZ')
auth=()
token=${GH_TOKEN:-${GITHUB_TOKEN:-}}
[[ -z $token ]] || auth=(-H "Authorization: Bearer $token")

releases() {
    local page=1 data message
    while :; do
        if ! data=$(curl --fail-with-body -sS --max-time 30 "${auth[@]}" \
            -H 'Accept: application/vnd.github+json' \
            "https://api.github.com/repos/$1/releases?per_page=100&page=$page"); then
            message=$(jq -r '.message // empty' <<< "$data" 2>/dev/null) || message=
            printf 'ERROR: %s: %s\n' "$1" "${message:-GitHub request failed}" >&2
            [[ $message != *rate*limit* ]] || echo 'Set GH_TOKEN/GITHUB_TOKEN, or wait for the API quota to reset.' >&2
            return 1
        fi
        if ! jq -e 'type == "array"' <<< "$data" >/dev/null; then
            echo "ERROR: $1: expected a GitHub releases array" >&2
            return 1
        fi
        printf '%s\n' "$data"
        [[ $(jq length <<< "$data") == 100 ]] || break
        page=$((page + 1))
    done
}

printf 'Release publication cutoff: %s\n' "$cutoff"
seen=' '
for repo in "$@"; do
    if [[ ! $repo =~ ^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$ \
        || ${repo#*/} == . || ${repo#*/} == .. ]]; then
        echo "Expected OWNER/REPO, not a version or URL: $repo" >&2
        exit 2
    fi
    [[ $seen != *" $repo "* ]] || continue
    seen+="$repo "
    # Fetch successfully before selection: no jq cascade or partial-page fallback.
    all_releases=$(releases "$repo") || exit 1
    release=$(jq -ser --arg cutoff "$cutoff" '
        add | map(
            select(.draft == false and .prerelease == false)
            | select((.published_at | fromdateiso8601) <= ($cutoff | fromdateiso8601))
            | . + {v: (.tag_name
                | capture("^v?(?<n>[0-9]+(\\.[0-9]+){0,2})(\\+[0-9A-Za-z.-]+)?$").n
                | split(".") | map(tonumber))}
        )
        | sort_by(.v[0], (.v[1] // 0), (.v[2] // 0), (.v | length), .published_at)
        | last // error("No eligible stable numeric GitHub Release")
        | [.tag_name, .published_at, (.immutable // false)] | @tsv
    ' <<< "$all_releases")
    IFS=$'\t' read -r version published immutable <<< "$release"
    sha=$(GIT_TERMINAL_PROMPT=0 git ls-remote "https://github.com/$repo.git" \
        "refs/tags/$version" "refs/tags/$version^{}" | awk '
        /\^\{\}$/ { peeled = $1; next } { tag = $1 }
        END { print peeled ? peeled : tag }
    ')
    [[ $sha =~ ^[0-9a-f]{40}$ ]] || { echo "Invalid/missing SHA for $repo $version" >&2; exit 1; }
    printf '\n%s | %s | published %s\n' "$repo" "$version" "$published"
    printf 'https://github.com/%s/releases/tag/%s\n' "$repo" "$version"
    printf 'uses: %s@%s # %s\n' "$repo" "$sha" "$version"
    if [[ $immutable != true ]]; then
        echo "WARNING: $repo $version is not reported immutable; publication age does not prove tag target age." >&2
    fi
done
