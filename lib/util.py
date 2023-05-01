import pathlib
import random
from datetime import datetime
from rich import print
import pyarrow as pa


class CodenameGen:
    STYLE_CAMEL = 'camel'
    STYLE_UPPER = 'uppercase'
    STYLE_LOWER = 'lowercase'
    STYLE_TITLE = 'title'

    def __init__(self, syllabus_file=pathlib.Path(__file__).parent / r'syllabus.txt', seed=None):
        with open(syllabus_file, 'r') as _sf:
            self._syllabus = [_s.strip().lower() for _s in _sf.readlines()]
        if seed is not None:
            random.seed(seed)

    def _next_syllabus(self, style) -> str:
        _ns = random.choice(self._syllabus)
        if style == self.STYLE_CAMEL:
            if len(_ns) == 1:  # try again, single-char syllabus are not allowed in camel-case
                return self._next_syllabus(style)
            _ns = _ns.capitalize()
        elif style == self.STYLE_UPPER:
            _ns = _ns.upper()
        return _ns

    def generate(self, syllabus_count=None, min_length=None, fixed_length=None, style=STYLE_LOWER) -> str:
        if min_length is not None and fixed_length is not None:
            raise ValueError(f'Conflicting arguments. The min-len and fixed-len can not be provided simultaneously')
        if style not in (self.STYLE_CAMEL, self.STYLE_UPPER, self.STYLE_LOWER, self.STYLE_TITLE):
            raise ValueError(f'The style is not recognized. Possible choices are: '
                             f'{[self.STYLE_CAMEL, self.STYLE_UPPER, self.STYLE_LOWER, self.STYLE_TITLE]}')
        if style == self.STYLE_CAMEL and fixed_length is not None and fixed_length < 4:
            raise ValueError('Generating camel-case is not possible if fixed length is less than 4 characters')
        if syllabus_count is not None and syllabus_count < 2:
            raise ValueError('By design the method generates at least two-syllabus words')
        if fixed_length is not None and fixed_length < 3:
            raise ValueError('By design the method generates words at least 3 characters long')
        if min_length is not None and min_length < 3:
            raise ValueError('By design the method generates words at least 3 characters long')
        if syllabus_count is not None and fixed_length is not None and fixed_length / syllabus_count <= 2.0:
            raise ValueError('If both fixed-length and syllabus-count are provided, the method will not attempt to '
                             'generate if ratio of aforementioned arguments does not exceed 2.0')
        if syllabus_count is not None and min_length is not None and min_length / syllabus_count <= 2.0:
            raise ValueError('If both min-length and syllabus-count are provided, the method will not attempt to '
                             'generate if ratio of aforementioned arguments does not exceed 2.0')

        if not syllabus_count and not min_length and not fixed_length:
            syllabus_count = 2
        if not min_length and not fixed_length:
            min_length = 3
        if fixed_length:
            min_length = fixed_length

        _word = [self._next_syllabus(style)]
        _rem_chars = min_length - len(_word[0])
        if fixed_length:
            while _rem_chars < 0:  # sometimes the syllabus length is greater than desired length
                _word = [self._next_syllabus(style)]
                _rem_chars = min_length - len(_word[0])

        while (_rem_chars > 0 and syllabus_count is None) or (syllabus_count is not None and len(_word) < syllabus_count):
            _ns = self._next_syllabus(style)
            if fixed_length and _rem_chars < len(_ns):
                if _rem_chars < 1:
                    break
                continue
            _word.append(_ns)
            _rem_chars -= len(_ns)
            if syllabus_count is not None and len(_word) > syllabus_count:
                # restart
                _word = [self._next_syllabus(style)]
                _rem_chars = min_length - len(_word[0])

        _word = ''.join(_word)
        if style == self.STYLE_TITLE:
            _word = _word.capitalize()
        return _word


# if __name__ == '__main__':
# copied from PDF: http://cts.vresp.com/c/?ManhattanStrategyGro/78604669ff/3eed2d48e9/b4fa0ae8b0
#     _syl_raw = '1. 2. 3. 4. 5. 6. 7. 8. 9. ing er i y ter al ed es e 10. tion 11. re 12. o 13. oth 14. ry 15. de 16. ver 17. ex 18. en 19. di 20. bout 21. com 22. ple 23. u 24. con 25. per 26. un 27. der 28. tle 29. ber 30. ty 31. num 32. peo 33. ble 34. af 35. ers 36. mer 37. wa 38. ment 39. pro 40. ar 41. ma 42. ri 43. sen 44. ture 45. fer 46. dif 47. pa 48. tions 49. ther 50. fore 89. ad 51. est 52. fa 53. la 54. ei 55. not 56. si 57. ent 58. ven 59. ev 60. ac 61. ca 62. fol 63. ful 64. na 65. tain 66. ning 67. col 68. par 69. dis 90. tween 91. gan 92. bod 70. ern 71. ny 72. cit 73. po 74. cal 75. mu 76. moth 77. pic 78. im 79. coun 80. mon 81. pe 82. lar 83. por 84. fi 85. bers 86. sec 87. ap 88. stud 93. tence 94. ward 95. hap 96. nev 97. ure 98. mem 99. ters 100. cov 101. ger  102. nit '
#
#     with open('syllabus.txt', 'w', newline='') as _file:
#         _file.writelines([_syl+'\n' for _syl in _syl_raw.split() if not re.match(r'\d+\.', _syl)])

KB = 1024
MB = KB*1024
GB = MB*1024


def _format_bytes(b: int):
    return f"{b/GB:.1f} GB" if b >= GB else f"{b/MB:.1f} MB" if b >= MB else f"{b/KB:.1f} KB"


def report_processing(action: str, _started_at: datetime, pa_table: pa.Table):
    print(f'[green]{action} in {(datetime.now() - _started_at).total_seconds():.1f} s, '
          f'{pa_table.num_rows} records, {_format_bytes(pa_table.nbytes)} '
          f'[green], total mem.: [red]{_format_bytes(pa.total_allocated_bytes())}')


