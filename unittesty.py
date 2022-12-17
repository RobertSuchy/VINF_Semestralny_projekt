from search import search
import unittest


class SearchUnitTest(unittest.TestCase):
    # test for searching motorcycles included in results file
    def test_search_existing_value(self):
        name = "BMW"
        manufacturer ="BMW" 
        moto_type = "Standard" 
        category = "standard" 
        transmission = "6"

        self.assertGreaterEqual(len(search(name, manufacturer, moto_type, category, transmission)), 1)

    # test for searching non existing motorcycle
    def test_search_non_existing_value(self):
        name = "test"
        manufacturer ="-" 
        moto_type = "-" 
        category = "-" 
        transmission = "8"

        self.assertEqual(len(search(name, manufacturer, moto_type, category, transmission)), 0)


if __name__ == '__main__':
    # running unit tests
    unittest.main()
