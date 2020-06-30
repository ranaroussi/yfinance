import unittest
import doctest
import sys
import os

def all_tests_suite():
    def get_suite():
        suite_names = [
            '%s' % (os.path.splitext(f)[0],)
            for f in os.listdir(os.path.dirname(__file__))
            if f.startswith('test_') and f.endswith('.py')
        ]
        try:
            # Applies function to list of inputs
            module = map(__import__, suite_names)
        except ImportError as IE:
            raise

        return unittest.TestLoader().loadTestsFromNames(suite_names)

    suite = get_suite()
    suite = unittest.TestSuite([suite])
    return suite


def main():
    runner = unittest.TextTestRunner(verbosity=1 + sys.argv.count('-v'))
    suite = all_tests_suite()
    raise SystemExit(not runner.run(suite).wasSuccessful())


if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    main()