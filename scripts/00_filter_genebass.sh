#!/usr/bin/env bash
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=filter_genebass
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/brava-ground-truth-annotations
#SBATCH --output=logs/filter_genebass.log
#SBATCH --error=logs/filter_genebass.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=21

set -eu

source utils/qsub_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data-ukb"
readonly in="${in_dir}/ukb_wes_450k.qced.chr${chr}.vcf.gz"

readonly out_dir="data/vep-in"
readonly out_prefix="${out_dir}/genebase.chr${chr}"

echo "ok"

mkdir -p ${out_dir}

readonly genebass_dir="/gpfs3/well/lindgren-ukbb/projects/ukbb-11867/nbaya/resources/genebass/"
readonly genebass_variants="${genebass_dir}/variant_results.rows.chr${chr}.tsv.gz"
readonly variants="${out_prefix}.varid.txt"
zcat ${genebass_variants} | awk ' {print $1":"$2":"$3":"$4}' > "${variants}" 

module load BCFtools/1.17-GCC-12.2.0
bcftools view -i "ID=@${variants}" ${in} -Oz -o ${out_prefix}.vcf.gz
bcftools index ${out_prefix}.vcf.gz






