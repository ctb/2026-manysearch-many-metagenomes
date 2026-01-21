ROCKSDB='/home/ctbrown/scratch3/2026-gtdb-dl/gtdb-cds-rs226.species.singleton.rocksdb'
DBMAG= '/home/ctbrown/scratch3/2025-workflow-core99/outputs.cds/cds3/clean-gtdb+bins.species.singleton.k21.rocksdb'

if 0:
    OUTPUT='david-shya'
    PATH="/home/ctbrown/scratch3/shya-david-data/sigs/"
    EXT=".sig.zip"
    METAG_LIST="david-shya-list.txt"
elif 0:
    OUTPUT='kunal-digesta'
    PATH='kunal-digesta/'
    EXT='.sig.zip'
    METAG_LIST='kunal-digesta-list.txt'
elif 1:
    OUTPUT='mattia-2k'
    PATH='/home/ctbrown/scratch3/2025-other-pig-bins/annie-dl/sketches_2000/6-1a-smash/'
    EXT='.sig.gz'
    METAG_LIST='mattia-2k-list.txt'
    

METAGS=[ x.strip() for x in open(METAG_LIST) ]
#METAGS=METAGS[:5]               # subselect first 5 for testing


rule all:
    input:
        expand('manysearch/{o}/{m}.manysearch.csv', o=[OUTPUT], m=METAGS)

rule manysearch:
    input:
        metag_sig=PATH+'{metag}'+EXT,
        db=DBMAG,
    output:
        csv='manysearch/{o}/{metag}.manysearch.csv',
    threads: 1
    shell: """
        sourmash scripts manysearch -k 21 --scaled=1000 --threshold=0 \
           {input.metag_sig} {input.db} -o {output.csv} -c {threads}
    """
