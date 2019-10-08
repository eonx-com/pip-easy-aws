import unittest

from EasyLog.Log import Log
from EasyValidator.EasyValidator import EasyValidator


class EasyValidatorTypeTest(unittest.TestCase):
    def test_validate_type_bool_failure(self) -> None:
        """
        Assert non-boolean values are not detected as boolean

        :return: None
        """
        Log.test('Asserting integer 1 is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 1})
        )

        Log.test('Asserting integer 0 is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 0})
        )

        Log.test('Asserting string True is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 'True'})
        )

        Log.test('Asserting string False is not a valid bool...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': 'False'})
        )

    def test_validate_type_bool_success(self) -> None:
        """
        Assert boolean values are detected as boolean

        :return: None
        """
        Log.test('Asserting boolean False is a valid bool...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': False})
        )

        Log.test('Asserting boolean True is a valid bool...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': bool, 'values': ['bool_test'], 'allow_none': False},
            data={'bool_test': True})
        )

    def test_validate_type_float_failure(self) -> None:
        """
        Assert non-float values are not detected as float

        :return: None
        """
        Log.test('Asserting integer 1 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 1})
        )

        Log.test('Asserting integer 0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 0})
        )

        Log.test('Asserting integer -1 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': -1})
        )

        Log.test('Asserting string 1.0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': '1.0'})
        )

        Log.test('Asserting string 0.0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': '0.0'})
        )

        Log.test('Asserting string -1.0 is not a valid float...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': '-1.0'})
        )

    def test_validate_type_float_success(self) -> None:
        """
        Assert float values are detected as float

        :return: None
        """
        Log.test('Asserting float 1.0 is a valid float...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 1.0})
        )

        Log.test('Asserting float 0.0 is a valid float...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': 0.0})
        )

        Log.test('Asserting float -1.0 is a valid float...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': float, 'values': ['float_test'], 'allow_none': False},
            data={'float_test': -1.0})
        )

    def test_validate_type_int_failure(self) -> None:
        """
        Assert non-integer values are not detected as integer

        :return: None
        """
        Log.test('Asserting boolean True is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': True})
        )

        Log.test('Asserting boolean False is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': False})
        )

        Log.test('Asserting string 1 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': '1'})
        )

        Log.test('Asserting string 0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': '0'})
        )

        Log.test('Asserting string -1 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': '-1'})
        )

        Log.test('Asserting float -1.0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': -1.0})
        )

        Log.test('Asserting float 0.0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 0.0})
        )

        Log.test('Asserting float 1.0 is not a valid integer...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 1.0})
        )

    def test_validate_type_int_success(self) -> None:
        """
        Assert integer values are detected as integer

        :return: None
        """
        Log.test('Asserting int 1 is a valid int...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 1})
        )

        Log.test('Asserting int 0 is a valid int...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': 0})

        )
        Log.test('Asserting int -1 is a valid int...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': int, 'values': ['int_test'], 'allow_none': False},
            data={'int_test': -1})
        )

    def test_validate_type_str_failure(self) -> None:
        """
        Assert non-string values are not detected as string

        :return: None
        """
        Log.test('Asserting boolean False is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': False})
        )

        Log.test('Asserting boolean True is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': True})
        )

        Log.test('Asserting integer 0 is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 0})
        )

        Log.test('Asserting integer 1 is not a valid string...')
        self.assertFalse(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 1})
        )

    def test_validate_type_str_success(self) -> None:
        """
        Assert string values are detected as string

        :return: None
        """
        Log.test('Asserting string is a valid string...')
        self.assertTrue(EasyValidator.validate_type(
            rule={'type': str, 'values': ['str_test'], 'allow_none': False},
            data={'str_test': 'This is a valid string'})
        )


if __name__ == '__main__':
    unittest.main()
