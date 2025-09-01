import duplicategenerator

dupgen =  duplicategenerator.DuplicateGen(
        #     num_org_records = 4000,
            num_org_records = 100000,
            num_dup_records = 11000,
            max_num_dups = 1,
            max_num_field_modifi= 1,
            max_num_record_modifi= 1,
            prob_distribution = "uniform",
            type_modification= "all",
            verbose_output = False,
            culture = "eng",
            attr_file_name = './attr_config_file.example.json',
            field_names_prob = {'culture' : 0,
                                'sex': 0,
                                'given_name':0.3,
                                'surname':0.3, 
                                'date_of_birth':0.15,
                                'phone_number':0.2,
                                'national_identifier':0.05}
        )


df = dupgen.generate("dataframe")
df
   