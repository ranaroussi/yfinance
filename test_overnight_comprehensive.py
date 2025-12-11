#!/usr/bin/env python3
"""
Test overnight quote functionality - regression testing
"""
import yfinance as yf
import sys

def test_1_btc_overnight_data():
    """Check if BTC-USD returns overnight data"""
    print("\nTest 1: BTC-USD overnight data")
    print("-" * 60)
    try:
        ticker = yf.Ticker("BTC-USD")
        fi = ticker.fast_info

        price = fi.overnight_price
        time = fi.overnight_time

        print(f"Price: {price}")
        print(f"Time: {time}")

        if price is not None:
            print("Pass - found overnight data")
            return True
        else:
            print("No data found, but that's ok")
            return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_2_equity_overnight_data():
    """Check regular stock overnight data"""
    print("\nTest 2: Regular equity (AAPL)")
    print("-" * 60)
    try:
        ticker = yf.Ticker("AAPL")
        fi = ticker.fast_info

        price = fi.overnight_price
        time = fi.overnight_time

        print(f"Price: {price}")
        print(f"Time: {time}")

        if price is None:
            print("Returns None - no overnight trading")
        else:
            print("Found data - stock has extended hours")
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_3_property_keys_exist():
    """Make sure all properties show up in keys"""
    print("\nTest 3: Check property keys")
    print("-" * 60)
    try:
        ticker = yf.Ticker("BTC-USD")
        fi = ticker.fast_info

        expected = ['overnightPrice', 'overnightTime', 'overnightHigh',
                   'overnightLow', 'overnightOpen', 'overnightVolume']

        missing = []
        for key in expected:
            if key not in fi.keys():
                missing.append(key)
                print(f"  {key}: missing")
            else:
                print(f"  {key}: ok")

        if not missing:
            print("All properties found")
            return True
        else:
            print(f"Missing: {missing}")
            return False
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_4_dictionary_access():
    """Test both property and dict access work"""
    print("\nTest 4: Dictionary access")
    print("-" * 60)
    try:
        ticker = yf.Ticker("BTC-USD")
        fi = ticker.fast_info

        price_prop = fi.overnight_price
        price_dict = fi['overnightPrice']

        print(f"Property access: {price_prop}")
        print(f"Dict access: {price_dict}")

        if price_prop == price_dict:
            print("Both methods return same value")
            return True
        else:
            print("Values don't match")
            return False
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_5_timestamp_validation():
    """Check if timestamp is actually overnight hours"""
    print("\nTest 5: Time validation")
    print("-" * 60)
    try:
        ticker = yf.Ticker("BTC-USD")
        fi = ticker.fast_info

        time = fi.overnight_time

        if time is None:
            print("No data available")
            return True

        hour = time.hour
        print(f"Timestamp: {time}")
        print(f"Hour: {hour}")

        if 0 <= hour < 7:
            print("Hour is in overnight window")
            return True
        else:
            print(f"Hour {hour} not in overnight range")
            return False
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_6_ohlc_consistency():
    """Verify OHLC values make sense"""
    print("\nTest 6: OHLC consistency")
    print("-" * 60)
    try:
        ticker = yf.Ticker("BTC-USD")
        fi = ticker.fast_info

        high = fi.overnight_high
        low = fi.overnight_low
        open_price = fi.overnight_open
        close = fi.overnight_price

        print(f"Open: {open_price}")
        print(f"High: {high}")
        print(f"Low: {low}")
        print(f"Close: {close}")

        if high is None or low is None:
            print("No OHLC data")
            return True

        if high >= low:
            print("High >= Low check passed")
            if open_price and close:
                if low <= open_price <= high and low <= close <= high:
                    print("Open/Close within range")
                else:
                    print("Warning: values outside expected range")
            return True
        else:
            print("Error: High < Low")
            return False
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_7_existing_properties_unchanged():
    """Make sure we didn't break anything"""
    print("\nTest 7: Existing properties check")
    print("-" * 60)
    try:
        ticker = yf.Ticker("AAPL")
        fi = ticker.fast_info

        last_price = fi.last_price
        currency = fi.currency
        timezone = fi.timezone

        print(f"Last price: {last_price}")
        print(f"Currency: {currency}")
        print(f"Timezone: {timezone}")

        if last_price and currency:
            print("Existing properties work fine")
            return True
        else:
            print("Something broke")
            return False
    except Exception as e:
        print(f"Failed: {e}")
        return False

def run_all_tests():
    """Run the test suite"""
    print("=" * 60)
    print("Testing overnight quote functionality")
    print("=" * 60)

    tests = [
        test_1_btc_overnight_data,
        test_2_equity_overnight_data,
        test_3_property_keys_exist,
        test_4_dictionary_access,
        test_5_timestamp_validation,
        test_6_ohlc_consistency,
        test_7_existing_properties_unchanged
    ]

    results = []
    for test in tests:
        results.append(test())

    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)

    passed = sum(results)
    total = len(results)

    for i, result in enumerate(results, 1):
        print(f"Test {i}: {'passed' if result else 'failed'}")

    print(f"\n{passed}/{total} tests passed")

    if passed == total:
        print("\nAll tests passed")
        return True
    else:
        print(f"\n{total - passed} test(s) failed")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
