### Test Case Document

**Purpose:**

Verify that the `.earnings` and `.quarterly_earnings` properties are able to generate and handle positive, negative and zero values

**Test Run Information:**

Tester Name: Spencer von der Ohe

Date(s) of Test: March 27th, 2021

**Prerequisites for this test:**
None

**Software Versions:**

Fork of finance: https://github.com/sazzy4o/yfinance

Python Version: Python 3.4+

**Required Configuration:**

No special setup needed

**Notes and Results:**

<table>
<thead>
<tr>
<th><strong>Step</strong></th>
<th><strong>Test Step/Input</strong></th>
<th><strong>Expected Results</strong></th>
<th><strong>Actual Results</strong></th>
<th><strong>Requirement Validation</strong></th>
<th><strong>Pass/Fail</strong></th>
</tr>
</thead>
<tbody>
<tr>
<td colspan="6">Flow 1: Verify that the annual <code>.earnings</code> property supports positive values</td>
</tr>
<tr>
<td>1</td>
<td>Replace <code>get_json</code> with mock function that returns a company with positive earnings</td>
<td><code>get_json</code> is updated</td>
<td><code>get_json</code> is updated</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>2</td>
<td>Create ticker that corrisponds to the mocked data</td>
<td>Ticker is created</td>
<td>Ticker is created</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>3</td>
<td>Get <code>earnings</code> property</td>
<td>Earning property is returned</td>
<td>Earning property is returned</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>4</td>
<td>Validate that the <code>earnings</code></td>
<td>Match expected positive value</td>
<td>Match expected positive value</td>
<td><code>earnings</code> can handle positive values</td>
<td>✅</td>
</tr>
<tr>
<td colspan="6">Flow 2: Verify that the <code>.quarterly_earnings</code> property supports positive values</td>
</tr>
<tr>
<td>1</td>
<td>Replace <code>get_json</code> with mock function that returns a company with positive quarterly earnings</td>
<td><code>get_json</code> is updated</td>
<td><code>get_json</code> is updated</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>2</td>
<td>Create ticker that corrisponds to the mocked data</td>
<td>Ticker is created</td>
<td>Ticker is created</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>3</td>
<td>Get <code>quarterly_earnings</code> property</td>
<td>Earning property is returned</td>
<td>Earning property is returned</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>4</td>
<td>Validate that the <code>quarterly_earnings</code></td>
<td>Match expected positive value</td>
<td>Match expected positive value</td>
<td><code>quarterly_earnings</code> can handle positive values</td>
<td>✅</td>
</tr>
<tr>
<td colspan="6">Flow 3: Verify that the annual <code>.earnings</code> property supports positive values</td>
</tr>
<tr>
<td>1</td>
<td>Replace <code>get_json</code> with mock function that returns a company with negative earnings</td>
<td><code>get_json</code> is updated</td>
<td><code>get_json</code> is updated</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>2</td>
<td>Create ticker that corrisponds to the mocked data</td>
<td>Ticker is created</td>
<td>Ticker is created</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>3</td>
<td>Get <code>earnings</code> property</td>
<td>Earning property is returned</td>
<td>Earning property is returned</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>4</td>
<td>Validate that the <code>earnings</code></td>
<td>Match expected negative value</td>
<td>Match expected negative value</td>
<td><code>earnings</code> can handle negative values</td>
<td>✅</td>
</tr>
<tr>
<td colspan="6">Flow 4: Verify that the <code>.quarterly_earnings</code> property supports negative values</td>
</tr>
<tr>
<td>1</td>
<td>Replace <code>get_json</code> with mock function that returns a company with negative quarterly earnings</td>
<td><code>get_json</code> is updated</td>
<td><code>get_json</code> is updated</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>2</td>
<td>Create ticker that corrisponds to the mocked data</td>
<td>Ticker is created</td>
<td>Ticker is created</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>3</td>
<td>Get <code>quarterly_earnings</code> property</td>
<td>Earning property is returned</td>
<td>Earning property is returned</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>4</td>
<td>Validate that the <code>quarterly_earnings</code></td>
<td>Match expected negative value</td>
<td>Match expected negative value</td>
<td><code>quarterly_earnings</code> can handle negative values</td>
<td>✅</td>
</tr>
<tr>
<td colspan="6">Flow 5: Verify that the annual <code>.earnings</code> property supports zero values</td>
</tr>
<tr>
<td>1</td>
<td>Replace <code>get_json</code> with mock function that returns a company with zero earnings</td>
<td><code>get_json</code> is updated</td>
<td><code>get_json</code> is updated</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>2</td>
<td>Create ticker that corrisponds to the mocked data</td>
<td>Ticker is created</td>
<td>Ticker is created</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>3</td>
<td>Get <code>earnings</code> property</td>
<td>Earning property is returned</td>
<td>Earning property is returned</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>4</td>
<td>Validate that the <code>earnings</code></td>
<td>Match expected zero value</td>
<td>Match expected zero value</td>
<td><code>earnings</code> can handle values of zero</td>
<td>✅</td>
</tr>
<tr>
<td colspan="6">Flow 6: Verify that the <code>.quarterly_earnings</code> property supports zero values</td>
</tr>
<tr>
<td>1</td>
<td>Replace <code>get_json</code> with mock function that returns a company with zero quarterly earnings</td>
<td><code>get_json</code> is updated</td>
<td><code>get_json</code> is updated</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>2</td>
<td>Create ticker that corrisponds to the mocked data</td>
<td>Ticker is created</td>
<td>Ticker is created</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>3</td>
<td>Get <code>quarterly_earnings</code> property</td>
<td>Earning property is returned</td>
<td>Earning property is returned</td>
<td></td>
<td>✅</td>
</tr>
<tr>
<td>4</td>
<td>Validate that the <code>quarterly_earnings</code></td>
<td>Match expected zero value</td>
<td>Match expected zero value</td>
<td><code>quarterly_earnings</code> can handle values of zero</td>
<td>✅</td>
</tr>
</tbody>
</table>