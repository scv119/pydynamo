import unittest
import doctest
import sys
import os


def all_tests_suite():
    suite_names = [
        'pydynamo.test.%s' % (os.path.splitext(f)[0],)
        for f in os.listdir(os.path.dirname(__file__))
        if f.endswith('test.py')
    ]
    return unittest.TestLoader().loadTestsFromNames(suite_names)


def main():
    runner = unittest.TextTestRunner(verbosity=1 + sys.argv.count('-v'))
    suite = all_tests_suite()
    raise SystemExit(not runner.run(suite).wasSuccessful())


if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    main()
