### Test Case Document

**Purpose:**

Verify that the `earnings` property is able to generate and handle positive values

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
| 1 | Replace `get_json` with mock function that returns a company with positive earnings | `get_json` is updated | `get_json` is updated |  | ✅ |
| 2 | Create ticker that corrisponds to the mocked data | Ticker is created |  Ticker is created |  | ✅ |
| 3 | Get `earnings` property | Earning property is returned | Earning property is returned |  | ✅ |
| 4 | Validate that the `earnings` | Match expected positive value| Match expected positive value | `earnings` can handle positive values |✅ |
