import argparse
from peer_assess_pro import PeerAssessPro
from sword import Sword

def arg_parse():
    parser = argparse.ArgumentParser(description='ETL Workflows')
    parser.add_argument('--directory', type=str, default='peer_assess_pro',
                        help='Workflow Directory')

    return parser

def extract_and_load(dirc):
    # sword = Sword(dirc)
    # sword.load_to_staging_warehouse()
    pap = PeerAssessPro(dirc)
    pap.load_to_staging_warehouse()
    

if __name__ == '__main__':
    args = arg_parse().parse_args()
    dirc = args.directory
    extract_and_load(dirc)
