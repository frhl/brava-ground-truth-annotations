#!/usr/bin/env bash
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=hail_ground_truth
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/for_nik
#SBATCH --output=logs/hail_ground_truth.log
#SBATCH --error=logs/hail_ground_truth.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=21

source utils/qsub_utils.sh
source utils/hail_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly vep_dir="/well/lindgren/barney/for_nik/data/vep/hail-vep105"
readonly vep="${vep_dir}/ukb_wes_450k.qced.chr${chr}.vep_with_gnomad.ht"

readonly out_dir="data/vep-hail"
readonly out_prefix="${out_dir}/ukb_wes_450k.qced.brava.v4.chr${chr}"
readonly hail_script="scripts/02_hail_ground_truth.py"

mkdir -p ${out_dir}
mkdir -p ${spark_dir}

set_up_hail 0.2.97
set_up_pythonpath_legacy  
python3 ${hail_script} \
     --vep_path "${vep}" \
     --out_prefix "${out_prefix}"




