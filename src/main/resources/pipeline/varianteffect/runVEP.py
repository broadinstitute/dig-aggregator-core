#!/usr/bin/python3

# NOTE: This script is run via script-runner, which REQUIRES that the script be
#       saved with Unix line-endings (LF, not CRLF)!

import argparse
import concurrent.futures
import glob
import os.path
import platform
import re
import shutil
import subprocess
import sys

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# local directory where 3rd party binaries and data are located
bindir = '/mnt/efs/bin'

# mounted docker volume path where local vep_data lives
voldir = '/opt/vep/.vep'

# fields in the INFO column of the VCF from the CSQ key
#
# see: https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html#vcf
#
csq_fields = [
    'Uploaded_variation',
    'Location',
    'Allele',
    'Gene',
    'Feature',
    'Feature_type',
    'Consequence',
    'cDNA_position',
    'CDS_position',
    'Protein_position',
    'Amino_acids',
    'Codons',
    'Existing_variation',
    'CCDS',
    'TSL',
    'APPRIS',
    'BIOTYPE',
    'CANONICAL',
    'HGNC',
    'ENSP',
    'DOMAINS',
    'MOTIF_NAME',
    'MOTIF_POS',
    'HIGH_INF_POS',
    'MOTIF_SCORE_CHANGE',
    'SIFT',
    'PolyPhen',
    'Condel',
    'IMPACT',
    'PICK',
]

# arguments to pass into the LoF plugin
lof_args = [
    'loftee_path:%s/Plugins/loftee-0.3-beta' % voldir,
    'human_ancestor_fa:%s/fasta/GRCh37.primary_assembly.genome.fa' % voldir,
]

# list of fields returned from LoF
lof_fields = [
    'LoF',
    'LoF_filter',
    'LoF_flags',
]

# list of fields from dbNSFP desired
dbnsfp_fields = [
    'CADD_phred',
    'CADD_raw_rankscore',
    'CADD_raw',
    'clinvar_clnsig',
    'clinvar_golden_stars',
    'clinvar_rs',
    'clinvar_trait',
    'DANN_rankscore',
    'DANN_score',
    'Eigen-PC-raw_rankscore',
    'Eigen-PC-raw',
    'Eigen-phred',
    'Eigen-raw',
    'FATHMM_converted_rankscore',
    'FATHMM_pred',
    'FATHMM_score',
    'fathmm-MKL_coding_group',
    'fathmm-MKL_coding_pred',
    'fathmm-MKL_coding_rankscore',
    'fathmm-MKL_coding_score',
    'GenoCanyon_score_rankscore',
    'GenoCanyon_score',
    'GERP++_NR',
    'GERP++_RS_rankscore',
    'GERP++_RS',
    'GM12878_confidence_value',
    'GM12878_fitCons_score_rankscore',
    'GM12878_fitCons_score',
    'GTEx_V6p_gene',
    'GTEx_V6p_tissue',
    'H1-hESC_confidence_value',
    'H1-hESC_fitCons_score_rankscore',
    'H1-hESC_fitCons_score',
    'HUVEC_confidence_value',
    'HUVEC_fitCons_score_rankscore',
    'HUVEC_fitCons_score',
    'integrated_confidence_value',
    'integrated_fitCons_score_rankscore',
    'integrated_fitCons_score',
    'Interpro_domain',
    'LRT_converted_rankscore',
    'LRT_Omega',
    'LRT_pred',
    'LRT_score',
    'MetaLR_pred',
    'MetaLR_rankscore',
    'MetaLR_score',
    'MetaSVM_pred',
    'MetaSVM_rankscore',
    'MetaSVM_score',
    'MutationAssessor_pred',
    'MutationAssessor_score_rankscore',
    'MutationAssessor_score',
    'MutationAssessor_UniprotID',
    'MutationAssessor_variant',
    'MutationTaster_AAE',
    'MutationTaster_converted_rankscore',
    'MutationTaster_model',
    'MutationTaster_pred',
    'MutationTaster_score',
    'phastCons100way_vertebrate_rankscore',
    'phastCons100way_vertebrate',
    'phastCons20way_mammalian_rankscore',
    'phastCons20way_mammalian',
    'phyloP100way_vertebrate_rankscore',
    'phyloP100way_vertebrate',
    'phyloP20way_mammalian_rankscore',
    'phyloP20way_mammalian',
    'Polyphen2_HDIV_pred',
    'Polyphen2_HDIV_rankscore',
    'Polyphen2_HDIV_score',
    'Polyphen2_HVAR_pred',
    'Polyphen2_HVAR_rankscore',
    'Polyphen2_HVAR_score',
    'PROVEAN_converted_rankscore',
    'PROVEAN_pred',
    'PROVEAN_score',
    'Reliability_index',
    'SIFT_converted_rankscore',
    'SIFT_pred',
    'SIFT_score',
    'SiPhy_29way_logOdds_rankscore',
    'SiPhy_29way_logOdds',
    'SiPhy_29way_pi',
    'Transcript_id_VEST3',
    'Transcript_var_VEST3',
    'VEST3_rankscore',
    'VEST3_score',
]

# arguments to pass into the dbNSFP plugin
dbnsfp_args = ['%s/dbNSFP/dbNSFP_hg19.gz' % voldir] + dbnsfp_fields


def create_container(data_path, out_path):
    """
    Create the container to run VEP in.
    """
    cmd = [
        'sudo',
        'docker',
        'run',

        # detached and interactive
        '-d',
        '-i',

        # mount input directory to /data in container
        '-v',
        '%s:/data' % data_path,

        # mount output directory to /out in container
        '-v',
        '%s:/out' % out_path,

        # mount VEP genome cache
        '-v',
        '%s/vep_data:%s' % (bindir, voldir),

        # the image name
        'ensemblorg/ensembl-vep',
    ]

    # run and return the container ID
    return subprocess.check_output(cmd).decode().strip()


def exec_container(container, *cmd, as_root=False):
    """
    Execute a shell command from within the container.
    """
    print('Executing in container %s: `%s`' % (container[:12], ' '.join(cmd)))

    # base command
    docker_cmd = ['sudo', 'docker', 'exec']

    # optionally run as root
    if as_root:
        docker_cmd += ['-u', 'root']

    # add the container name
    docker_cmd.append(container)

    # run it
    subprocess.check_call(docker_cmd + list(cmd))


def stop_container(container):
    """
    Stop and remove the container.
    """
    subprocess.check_call(['sudo', 'docker', 'stop', container])
    subprocess.check_call(['sudo', 'docker', 'rm', container])
    print('Stopped container %s' % container[:12])


def run_vep_in_docker(data_pathname, output_pathname):
    """
    Construct the command line for VEP to run in Docker.
    """
    data_path, data_file = os.path.split(data_pathname)
    out_path, out_file = os.path.split(output_pathname)

    # initialize and start the container
    container = create_container(data_path, out_path)
    print('Created container: ' + container[:12])

    # software to install into PATH
    programs = [
        '%s/samtools-1.9/samtools' % voldir,
    ]

    # where in the PATH to install the programs (must have privs)
    #path = '/opt/vep/src/ensembl-vep'
    path = '/usr/local/bin'

    # install samtools somewhere in PATH
    for program in programs:
        exec_container(container, 'ln', '-s', program, path, as_root=True)

    # VEP command
    vep = [
        'perl',
        '-I',
        '%s/Plugins/loftee-0.3-beta' % voldir,

        # run VEP in offline mode (use cache)
        './vep',
        '--fork',
        '16',
        '--json',
        '--no_stats',
        '--fasta',
        '%s/fasta/GRCh37.primary_assembly.genome.fa' % voldir,
        '--polyphen',
        'b',
        '--sift',
        'b',
        '--ccds',
        '--canonical',
        '--appris',
        '--tsl',
        '--biotype',
        '--regulatory',
        '--plugin',
        'dbNSFP,%s' % ','.join(dbnsfp_args),
        '--plugin',
        'LoF,%s' % ','.join(lof_args),
        '-offline',
        '--flag_pick_allele',
        '--pick_order',
        'tsl,biotype,appris,rank,ccds,canonical,length',
        '--domains',
        'flags',
        '--fields',
        ','.join(csq_fields + lof_fields + dbnsfp_fields),
        '-i',
        '/data/%s' % data_file,
        '-o',
        '/out/%s' % out_file,
    ]

    # run it then stop the container
    exec_container(container, *vep)
    stop_container(container)


def run_vep(part):
    """
    Run VEP over an input source file.
    """
    out_dir, out_name = os.path.split(part)
    out_basename, _ = os.path.splitext(out_name)

    # VEP outputs a JSON, which is copied back to S3
    out_json = '%s/%s.json' % (out_dir, out_basename)

    # execute VEP
    run_vep_in_docker(part, out_json)

    # return the output filename
    return out_json


if __name__ == '__main__':
    """
    @param dataset e.g. `GWAS_CAMP`
    @param part e.g. `part-00000-bbda408e-aa30-4959-9bbd-7b8350cc3088-c000.csv`
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    opts = argparse.ArgumentParser()
    opts.add_argument('part')

    # parse the command line parameters
    args = opts.parse_args()

    # source location in S3 where the input file is and the output goes
    src = '%s/variants/%s' % (s3dir, args.part)
    out = '%s/effects/%s' % (s3dir, args.part)

    # copy the part file to the local, working directory for processing
    subprocess.check_call(['aws', 's3', 'cp', src, '.'])

    # docker needs permissions to write to the output directory
    subprocess.check_call(['chmod', 'a+w', '.'])

    # get the absolute path to the local file
    local_part = os.path.abspath(args.part)

    # run VEP, on the local file
    outfile = run_vep(local_part)
    warnings = outfile + '_warnings.txt'

    # copy the output file back to S3
    subprocess.check_call(['aws', 's3', 'cp', outfile, out])

    # check if a warnings file exists, and copy that as well
    if os.path.exists(warnings):
        subprocess.check_call(['aws', 's3', 'cp', warnings, out + '_warnings.txt'])

    # delete the input files so as to not cause problems
    os.remove(local_part)
    os.remove(outfile + '*')
