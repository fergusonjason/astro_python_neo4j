
import unittest

from util import get_greek_letter

class TestGetGreekLetter(unittest.TestCase):

    def test_simple(self):

        input = "alp"
        result = get_greek_letter(input)

        self.assertEqual(result, "Alpha", "Expected Alpha, actual {}".format(result))

    def test_superscript(self):

        input = "gam01"
        result = get_greek_letter(input)

        self.assertEqual(result,"Gamma¹")

if __name__ == "__main__":
    unittest.main()