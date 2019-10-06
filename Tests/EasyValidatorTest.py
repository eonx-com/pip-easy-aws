import unittest

from EasyLambda.EasyValidator import EasyValidator


class EasyValidatorTest(unittest.TestCase):
    def test_validate_type_bool(self):
        """
        Ensure type tests fail when given a non-bool type
        """
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 'not_a_bool'})
        )
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 10})
        )

    def test_validate_type_str(self):
        """
        Ensure type tests fail when given a non-str type
        """
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 'this is a test'})
        )

    def test_validate_type_int(self):
        """
        Ensure type tests fail when given a non-int type
        """
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 10})
        )

    def test_validate_type_float(self):
        """
        Ensure type tests fail when given a non-float type
        """
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 10.1})
        )

    def test_validate_type_bool_failure(self):
        """
        Ensure type tests fail when given a non-bool type
        """
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 1})
        )
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 'False'})
        )

    def test_validate_type_str_failure(self):
        """
        Ensure type tests fail when given a non-str type
        """
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': False})
        )

    def test_validate_type_int_failure(self):
        """
        Ensure type tests fail when given a non-int type
        """
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 'not_int'})
        )

    def test_validate_type_float_failure(self):
        """
        Ensure type tests fail when given a non-float type
        """
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 'not_float'})
        )


if __name__ == '__main__':
    unittest.main()
