import os
import sys
sys.path.insert(0, os.path.abspath('../..')) 

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'yfinance / Pythonic access to market data'
copyright = '2017-2025 Ran Aroussi'
author = 'Ran Aroussi'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.napoleon',
              "sphinx.ext.githubpages",
              "sphinx.ext.autosectionlabel",
              "sphinx.ext.autosummary",
              "sphinx_copybutton"]

templates_path = ['_templates']
exclude_patterns = []
autoclass_content = 'both'
autosummary_generate = True
autodoc_default_options = {
    'exclude-members': '__init__',
    'members': True,
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_title = 'yfinance'
html_theme = 'pydata_sphinx_theme'
html_theme_options = {
    "github_url": "https://github.com/ranaroussi/yfinance",
    "navbar_align": "left",
    "logo": {
      "image_light": "_static/logo-light.webp",
      "image_dark": "_static/logo-dark.webp"
   }
}
html_static_path = ['_static']
html_css_files = ['yfinance.css']
