### Test Case Document

**Purpose:**

Verify that the `earnings` property is able to generate and handle values of zero

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
| 1 | Replace `get_json` with mock function that returns a company with zero earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created | `get_json` is updated |  | ✅ |
| 3 | Get `earnings` property | Ticker is created | `get_json` is updated |  | ✅ |
| 4 | Validate that the `earnings` match expected zero value| Ticker is created | `earnings` can handle values of zero | `get_json` is updated | ✅ |
