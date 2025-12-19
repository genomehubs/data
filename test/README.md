# BoaT update

Searches for processed assemblies on Lustre and OpenStack

## Isopoda

[tax_tree(29979) AND assembly_level = chromosome,complete genome AND biosample_representative = primary](https://goat.genomehubs.org/search?query=tax_tree%2829979%29%20AND%20assembly_level%20%3D%20chromosome%2Ccomplete%20genome%20AND%20biosample_representative%20%3D%20primary&result=assembly&includeEstimates=true&taxonomy=ncbi&offset=0&fields=assembly_span%2Cchromosome_count%2Cassembly_level&names=assembly_name)

## Lepidoptera

[tax_tree(7088) AND assembly_level = chromosome,complete genome AND biosample_representative = primary](https://goat.genomehubs.org/search?query=tax_tree%287088%29%20AND%20assembly_level%20%3D%20chromosome%2Ccomplete%20genome%20AND%20biosample_representative%20%3D%20primary&result=assembly&includeEstimates=true&taxonomy=ncbi&offset=0&fields=assembly_span%2Cchromosome_count%2Cassembly_level&names=assembly_name)

## Mollusca

[tax_tree(6447) AND assembly_level = chromosome,complete genome AND biosample_representative = primary](https://goat.genomehubs.org/search?query=tax_tree%286447%29%20AND%20assembly_level%20%3D%20chromosome%2Ccomplete%20genome%20AND%20biosample_representative%20%3D%20primary&result=assembly&includeEstimates=true&taxonomy=ncbi&offset=0&fields=assembly_span%2Cchromosome_count%2Cassembly_level&names=assembly_name)


## Others

[tax_tree(2759,!6447,!7088,!29979) AND assembly_level = chromosome,complete genome AND biosample_representative = primary](https://goat.genomehubs.org/search?query=tax_tree%282759%2C%216447%2C%217088%2C%2129979%29%20AND%20assembly_level%20%3D%20chromosome%2Ccomplete%20genome%20AND%20biosample_representative%20%3D%20primary&result=assembly&includeEstimates=true&taxonomy=ncbi&offset=0&fields=assembly_span%2Cchromosome_count%2Cassembly_level&names=assembly_name)


```
module load ISG/experimental/irods/4.2.7 
```

```
nano ~/.irods/irods_environment_btkarchive.json

{
  "irods_cwd": "/archive/tol/teams/blaxter/projects/btk_pipeline_completed",
  "irods_home": "/archive/home/btkarchive ",
  "irods_host": "irods-archive-ies1.internal.sanger.ac.uk",
  "irods_port": 1247,
  "irods_user_name": "btkarchive",
  "irods_zone_name": "archive",
  "irods_authentication_file": "/nfs/users/nfs_r/rc28/.irods/.irodsA_btkarchive",
  "irods_encryption_key_size": 32,
  "irods_encryption_salt_size": 8,
  "irods_encryption_num_hash_rounds": 16,
  "irods_encryption_algorithm": "AES-256-CBC",
  "irods_client_server_negotiation": "request_server_negotiation",
  "irods_client_server_policy": "CS_NEG_REQUIRE"
}

chmod 700 ~/.irods
chmod 600 ~/.irods/*

module load ISG/experimental/irods/4.2.7
export IRODS_ENVIRONMENT_FILE=$HOME/.irods/irods_environment_btkarchive.json
iinit
```

```

mbMem=100; bsub -n 1 -q normal -R"span[hosts=1] select[mem>${mbMem}] rusage[mem=${mbMem}]" -M${mbMem} -Is bash
```