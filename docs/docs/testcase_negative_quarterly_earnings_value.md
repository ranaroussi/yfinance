### Test Case Document

**Purpose:**

Verify that the `quarterly_earnings` property is able to generate and handle negative values

**Test Run Information:**

Tester Name: Spencer von der Ohe

Date(s) of Test: March 27th, 2021

**Prerequisites for this test:**
None

**Software Versions:**

Commit: 90c157344931c137777c0f10e2403192f174eacc

Package version: 0.1.55

OS: Windows 10

Python Version: 3.9.1

**Required Configuration:**

No special setup needed

**Notes and Results:**

| **Step**     | **Test Step/Input** | **Expected Results** | **Actual Results** | **Requirement Validation** | **Pass/Fail** |
| ------------ | ------------------- | -------------------- | ------------------ | -------------------------- | ------------- |
| Replace `get_json` with mock function that returns a company with negative quarterly earnings | `get_json` is updated |  | `get_json` is updated |  | ✅ |
| Create ticker that corrisponds to the mocked data | Ticker is created |  | `get_json` is updated |  | ✅ |
| Get `quarterly_earnings` property | Ticker is created |  | `get_json` is updated |  | ✅ |
| Validate that the `quarterly_earnings` match expected negative value| Ticker is created | `quarterly_earnings` can handle negative values | `get_json` is updated |  | ✅ |
