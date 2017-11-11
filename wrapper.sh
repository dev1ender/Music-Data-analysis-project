#! /bin/bash

sh /home/cloudera/Assignment/musicProject/scripts/start-demons.sh

sh /home/cloudera/Assignment/musicProject/scripts/populate-lookup.sh

sh /home/cloudera/Assignment/musicProject/scripts/dataformatting.sh

sh /home/cloudera/Assignment/musicProject/scripts/data_enrichment_filtering_schema.sh

sh /home/cloudera/Assignment/musicProject/scripts/data_enrichment.sh

sh /home/cloudera/Assignment/musicProject/scripts/data_analysis.sh

