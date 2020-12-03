import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['wines', 'reviews'])

    group = parser.add_mutually_exclusive_group(required=True)
    # wines = group.add_argument_group()
    # wines.add_argument("-f", "--from-year", metavar='C', type=int, action='store_const')
    # wines.add_argument("-t", "--to-year", metavar='B', type=int, action='store_const')
    #
    # reviews = group.add_argument_group()
    # reviews.add_argument("-y", "--year", metavar='A', type=int, action='store_const')

    # group.add

    # group.add_argument("-r", "--reviews", action="store_true")

    # parser.add_argument("square", type=int,
    #                     help="display a square of a given number")
    # parser.add_argument("-v", "--verbosity", type=int,
    #                     help="increase output verbosity")
    args = parser.parse_args()
