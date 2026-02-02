#ROCKSDB='/home/ctbrown/scratch3/2026-gtdb-dl/gtdb-cds-rs226.species.singleton.rocksdb'
#DBMAG= '/home/ctbrown/scratch3/2025-workflow-core99/outputs.cds/cds3/clean-gtdb+bins.species.singleton.k21.rocksdb'

OUTPUT=config['output_name'].rstrip('/')
PATH=config['sigs_path'].rstrip('/') + '/'
EXT=config['sigs_ext']
METAG_LIST=config['sigs_list']
DB=config['database']

print("")
print(f"config settings (specify using --configfile):")
print(f"    list of metagenome accessions/prefixes: '{METAG_LIST}'")
print(f"    metagenome sigs located at: '{PATH}'")
print(f"    metagenome sigs extension: '{EXT}'")
print(f"    output directory name: 'manysearch/{OUTPUT}/'")
print("")
print(f"searching against database: '{DB}'")

METAGS=[ x.strip() for x in open(METAG_LIST) ]
print("---")
print(f"loaded {len(METAGS)} metagenomes from '{METAG_LIST}'")

if config.get('subset'):
    SUBSET=int(config['subset'])
    METAGS=METAGS[:SUBSET]               # subselect first 5 for testing
    print(f"subsetting to {SUBSET} only per config => {len(METAGS)} metagenomes")
print("---")


rule all:
    input:
        expand('manysearch/{o}/{m}.manysearch.csv', o=[OUTPUT], m=METAGS)

rule manysearch:
    input:
        metag_sig=PATH+'{metag}'+EXT,
        db=DB,
    output:
        csv='manysearch/{o}/{metag}.manysearch.csv',
    threads: 32
    shell: """
        sourmash scripts manysearch -k 21 --scaled=1000 --threshold=0 \
           {input.db} {input.metag_sig} -o {output.csv} -c {threads}
    """
