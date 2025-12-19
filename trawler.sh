#!/bin/bash
# trawler.sh
# Usage: trawler.sh input.csv output.csv
#BSUB -o trawler-%J-output.log
#BSUB -e trawler-%J-error.log
#BSUB -J trawler
#BSUB -q normal
#BSUB -G team301
#BSUB -n 1
#BSUB -M 100
#BSUB -R "select[mem>100] rusage[mem=100]"

input_csv="$1"
output_csv="$2"

rm -f "$output_csv $output_csv.finished"

source /etc/profile
module load speciesops

# Output header
echo "taxon_id,assembly_name,lustre_path,busco_dirs" > "$output_csv"

extra_dir=/lustre/scratch127/tol/teams/blaxter/users/rc28/irods/buscos

while IFS=, read -r taxon_id assembly_name; do
    taxon_id=$(echo "$taxon_id" | tr -d '\r\n' | xargs)
    assembly_name=$(echo "$assembly_name" | tr -d '\r\n' | xargs)
    if [ -d "$extra_dir/analysis/$assembly_name" ]; then
        dir=$extra_dir
    else
        dir=$(speciesops getdir --taxon_id "$taxon_id" | grep /lustre | sed 's/^Species directory path: //')
    fi
    busco_dirs=$(ls -d "$dir/analysis/$assembly_name/busco/"*_odb*/ 2>/dev/null | xargs -n1 basename | tr '\n' ';')
    echo "$taxon_id,$assembly_name,$dir,$busco_dirs" >> "$output_csv"
done < "$input_csv"

touch "$output_csv.finished"