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

(Commit hash: 90c157344931c137777c0f10e2403192f174eacc)

Python Version: Python 3.4+

**Required Configuration:**

No special setup needed

**Notes and Results:**

| **Step**     | **Test Step/Input** | **Expected Results** | **Actual Results** | **Requirement Validation** | **Pass/Fail** |
| ------------ | ------------------- | -------------------- | ------------------ | -------------------------- | ------------- |
| Flow 1: Verify that the annual `.earnings` property supports positive values
| 1 | Replace `get_json` with mock function that returns a company with positive earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created |  Ticker is created |  | ✅ |
| 3 | Get `earnings` property | Earning property is returned | Earning property is returned |  | ✅ |
| 4 | Validate that the `earnings` | Match expected positive value| Match expected positive value | `earnings` can handle positive values |✅ |
| Flow 2: Verify that the `.quarterly_earnings` property supports positive values
| 1 | Replace `get_json` with mock function that returns a company with positive quarterly earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created | Ticker is created |  | ✅ |
| 3 | Get `quarterly_earnings` property | Earning property is returned | Earning property is returned |  | ✅ |
| 4 | Validate that the `quarterly_earnings`| Match expected positive value |Match expected positive value | `quarterly_earnings` can handle positive values | ✅ |
| Flow 3: Verify that the annual `.earnings` property supports positive values
| 1 | Replace `get_json` with mock function that returns a company with negative earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created | Ticker is created |  | ✅ |
| 3 | Get `earnings` property | Earning property is returned | Earning property is returned |  | ✅ |
| 4 | Validate that the `earnings` | Match expected negative value | Match expected negative value|`earnings` can handle negative values | ✅ |
| Flow 4: Verify that the `.quarterly_earnings` property supports negative values
| 1 | Replace `get_json` with mock function that returns a company with negative quarterly earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created | Ticker is created |  | ✅ |
| 3 | Get `quarterly_earnings` property | Earning property is returned | Earning property is returned  |  | ✅ |
| 4 | Validate that the `quarterly_earnings`| Match expected negative value | Match expected negative value| `quarterly_earnings` can handle negative values |✅ |
| Flow 5: Verify that the annual `.earnings` property supports zero values
| 1 | Replace `get_json` with mock function that returns a company with zero earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created | Ticker is created |  | ✅ |
| 3 | Get `earnings` property | Earning property is returned | Earning property is returned |  | ✅ |
| 4 | Validate that the `earnings` | Match expected zero value |Match expected zero value| `earnings` can handle values of zero | ✅ |
| Flow 6: Verify that the `.quarterly_earnings` property supports zero values
| 1 | Replace `get_json` with mock function that returns a company with zero quarterly earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created | Ticker is created |  | ✅ |
| 3 | Get `quarterly_earnings` property | Earning property is returned | Earning property is returned |  | ✅ |
| 4 | Validate that the `quarterly_earnings` | Match expected zero value | Match expected zero value| `quarterly_earnings` can handle values of zero |✅ |
