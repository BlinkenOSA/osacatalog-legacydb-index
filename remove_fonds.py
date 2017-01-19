import sys
import argparse
from common_config import solr_interface


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fonds", help="Number of fonds")
    args = parser.parse_args()
    args_fonds = args.fonds

    if args_fonds:
        solr_interface.delete(queries=solr_interface.Q(fonds_esort=args_fonds))
        solr_interface.commit()
        solr_interface.optimize()

    print "Finished!"

if __name__ == '__main__':
    sys.exit(main())
