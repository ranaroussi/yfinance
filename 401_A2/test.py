import isolated_parse_action

import unittest
class Test_parse(unittest.TestCase):
    """
    test parse_actions function
    """
    def test_null(self):
        """
        Test when the data passed in is None value
        """
        data=None
        result=isolated_parse_action.parse_actions(data)
        self.assertEqual(result,None)
    
if __name__ == '__main__':
    unittest.main()