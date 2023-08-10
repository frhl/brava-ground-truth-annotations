#!/usr/bin/env bash
#
#SBATCH --account=lindgren.prj
#SBATCH --job-name=filter_genebass
#SBATCH --chdir=/well/lindgren-ukbb/projects/ukbb-11867/flassen/projects/KO/brava-ground-truth-annotations
#SBATCH --output=logs/filter_genebass.log
#SBATCH --error=logs/filter_genebass.errors.log
#SBATCH --partition=short
#SBATCH --cpus-per-task 1
#SBATCH --array=1,21

set -eu

source utils/qsub_utils.sh

readonly spark_dir="data/tmp/spark"

readonly array_idx=$( get_array_task_id )
readonly chr=$( get_chr ${array_idx} )

readonly in_dir="data-ukb"
readonly in="${in_dir}/ukb_wes_450k.qced.chr${chr}.vcf.gz"

readonly out_dir="data/vep-in"
readonly out_prefix="${out_dir}/genebass.chr${chr}"

echo "ok"

mkdir -p ${out_dir}

readonly genebass_dir="/gpfs3/well/lindgren-ukbb/projects/ukbb-11867/nbaya/resources/genebass/"
readonly genebass_variants="${genebass_dir}/variant_results.rows.chr${chr}.tsv.gz"
readonly variants_all="${out_prefix}.varid.txt"
readonly variants_protein_coding="${out_prefix}.protein_coding.varid.txt"

zcat ${genebass_variants} | awk ' {print $1":"$2":"$3":"$4}' > "${variants_all}" 
zcat ${genebass_variants} | grep -E "(pLoF)|(missense)" | awk ' {print $1":"$2":"$3":"$4}' > "${variants_protein_coding}" 

module load BCFtools/1.17-GCC-12.2.0
bcftools index -f ${in}

bcftools view -i "ID=@${variants_all}" ${in} -O -o ${out_prefix}.vcf 
bgzip ${out_prefix}.vcf
bcftools index ${out_prefix}.vcf.gz

bcftools view -i "ID=@${variants_protein_coding}" ${in} -O -o ${out_prefix}.protein_coding.vcf
bgzip ${out_prefix}.protein_coding.vcf
bcftools index ${out_prefix}.protein_coding.vcf.gz

gzip -f ${variants_all}
gzip -f ${variants_protein_coding}
rm -f ${variants_all} ${variants_protein_coding}




