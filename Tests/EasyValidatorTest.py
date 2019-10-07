import unittest

from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyValidator import EasyValidator


class EasyValidatorTest(unittest.TestCase):
    def test_validate_type_bool_failure(self):
        """
        Assert non-boolean values are not detected as boolean
        """
        EasyLog.test('Asserting integer 1 is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 1})
        )

        EasyLog.test('Asserting integer 0 is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 0})
        )

        EasyLog.test('Asserting string True is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 'True'})
        )

        EasyLog.test('Asserting string False is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 'False'})
        )

    def test_validate_type_bool_success(self):
        """
        Assert boolean values are detected as boolean
        """
        EasyLog.test('Asserting boolean False is a valid bool...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': False})
        )

        EasyLog.test('Asserting boolean True is a valid bool...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': True})
        )

    def test_validate_type_float_failure(self):
        """
        Assert non-float values are not detected as float
        """
        EasyLog.test('Asserting integer 1 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 1})
        )

        EasyLog.test('Asserting integer 0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 0})
        )

        EasyLog.test('Asserting integer -1 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': -1})
        )

        EasyLog.test('Asserting string 1.0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': '1.0'})
        )

        EasyLog.test('Asserting string 0.0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': '0.0'})
        )

        EasyLog.test('Asserting string -1.0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': '-1.0'})
        )

    def test_validate_type_float_success(self):
        """
        Assert float values are detected as float
        """
        EasyLog.test('Asserting float 1.0 is a valid float...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 1.0})
        )

        EasyLog.test('Asserting float 0.0 is a valid float...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 0.0})
        )

        EasyLog.test('Asserting float -1.0 is a valid float...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': -1.0})
        )

    def test_validate_type_int_failure(self):
        """
        Assert non-integer values are not detected as integer
        """
        EasyLog.test('Asserting boolean True is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': True})
        )

        EasyLog.test('Asserting boolean False is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': False})
        )

        EasyLog.test('Asserting string 1 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': '1'})
        )

        EasyLog.test('Asserting string 0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': '0'})
        )

        EasyLog.test('Asserting string -1 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': '-1'})
        )

        EasyLog.test('Asserting float -1.0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': -1.0})
        )

        EasyLog.test('Asserting float 0.0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 0.0})
        )

        EasyLog.test('Asserting float 1.0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 1.0})
        )

    def test_validate_type_int_success(self):
        """
        Assert integer values are detected as integer
        """
        EasyLog.test('Asserting int 1 is a valid int...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 1})
        )

        EasyLog.test('Asserting int 0 is a valid int...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 0})

        )
        EasyLog.test('Asserting int -1 is a valid int...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': -1})
        )

    def test_validate_type_str_failure(self):
        """
        Assert non-string values are not detected as string
        """
        EasyLog.test('Asserting boolean False is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': False})
        )

        EasyLog.test('Asserting boolean True is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': True})
        )

        EasyLog.test('Asserting integer 0 is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 0})
        )

        EasyLog.test('Asserting integer 1 is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 1})
        )

    def test_validate_type_str_success(self):
        """
        Assert string values are detected as string
        """
        EasyLog.test('Asserting string is a valid string...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 'This is a valid string'})
        )


if __name__ == '__main__':
    unittest.main()
