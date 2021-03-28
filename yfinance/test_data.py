import unittest, utils
from data import generic_patterns

class TestGenericPatterns(unittest.TestCase):
    def test_is_dict(self):
        empty_dict = dict()
        data = generic_patterns(empty_dict)
        self.assertTrue(isinstance(data, dict))
    
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()
    unittest.main()