from unittest import main
from unittest import TestCase

from lib.util import CodenameGen


class CodenameGenTests(TestCase):

    def setUp(self) -> None:
        self._gen = CodenameGen()

    def _basic_validation(self, codename: str, expected_style=CodenameGen.STYLE_LOWER):
        if codename is None or len(codename) == 0:
            self.fail(f'The generated codename is empty')
        if not codename.isalpha():
            self.fail(f'The generated codename "{codename}" is not alphabetic string')

        match expected_style:
            case CodenameGen.STYLE_LOWER:
                self.assertTrue(codename.islower(), f'The generated codename "{codename}" is not lowercase')
            case CodenameGen.STYLE_UPPER:
                self.assertTrue(codename.isupper(), f'The generated codename "{codename}" is not uppercase')
            case CodenameGen.STYLE_TITLE:
                self.assertTrue(codename.istitle(), f'The generated codename "{codename}" is not title-case')
            case CodenameGen.STYLE_CAMEL:
                self.assertTrue(not codename.istitle() and not codename.isupper() and not codename.islower(),
                                f'The generated codename "{codename}" is not camel-case')

    def test_parameterless(self):
        _codenames = [self._gen.generate() for _ in range(100)]
        for _cn in _codenames:
            self._basic_validation(_cn)

        print(f'OK: {", ".join(_codenames[:10])}')

    def test_minlength(self):
        for _len in range(4, 10):
            _codenames = [self._gen.generate(min_length=_len) for _ in range(100)]
            for _cn in _codenames:
                self._basic_validation(_cn)
                self.assertGreaterEqual(len(_cn), _len, f'The generated word "{_cn}" has length '
                                                        f'less then minimal ({_len})')
            print(f'OK: {", ".join(_codenames[:10])}')

    def test_letter_case(self):
        for _option in (CodenameGen.STYLE_LOWER, CodenameGen.STYLE_UPPER, CodenameGen.STYLE_TITLE):
            _codenames = [self._gen.generate(style=_option) for _ in range(100)]
            for _cn in _codenames:
                self._basic_validation(_cn, expected_style=_option)

            print(f'OK: {", ".join(_codenames[:10])}')

        _codenames = [self._gen.generate(style=CodenameGen.STYLE_CAMEL, syllabus_count=3) for _ in range(100)]
        for _cn in _codenames:
            self._basic_validation(_cn, expected_style=CodenameGen.STYLE_CAMEL)

        print(f'OK: {", ".join(_codenames[:10])}')

    def test_explore_limits(self):
        self._basic_validation(self._gen.generate(fixed_length=4000))
        self._basic_validation(self._gen.generate(fixed_length=2000, syllabus_count=990))
        self._basic_validation(self._gen.generate(style=CodenameGen.STYLE_CAMEL, min_length=70, syllabus_count=34),
                               expected_style=CodenameGen.STYLE_CAMEL)

    def test_idiot_durability(self):
        self.assertRaises(ValueError, self._gen.generate, min_length=5, fixed_length=4)
        self.assertRaises(ValueError, self._gen.generate, min_length=-1)
        self.assertRaises(ValueError, self._gen.generate, min_length=2)
        self.assertRaises(ValueError, self._gen.generate, fixed_length=-1)
        self.assertRaises(ValueError, self._gen.generate, fixed_length=2)
        self.assertRaises(ValueError, self._gen.generate, syllabus_count=0)
        self.assertRaises(ValueError, self._gen.generate, syllabus_count=1)
        self.assertRaises(ValueError, self._gen.generate, style='tion, verla')
        self.assertRaises(ValueError, self._gen.generate, style=CodenameGen.STYLE_CAMEL, fixed_length=3)
        self.assertRaises(ValueError, self._gen.generate, fixed_length=6, syllabus_count=3)
        self.assertRaises(ValueError, self._gen.generate, min_length=8, syllabus_count=4)


if __name__ == '__main__':
    main()
