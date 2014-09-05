# coding: utf8
# response.generic_patterns = ['.html']

Series = db.define_table('series',
                         Field('gse_name', 'text'),
                         format='%(gse_name)s',
                         migrate='series.table'
)

Series_Attribute = db.define_table('series_attribute',
                                   Field('series_id', 'reference series'),
                                   Field('attribute_name', 'text'),
                                   Field('attribute_value', 'text', 'text'),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='series_attribute.table'
)

Platform = db.define_table('platform',
                           Field('gpl_name', 'text'),
                           format='%(gpl_name)s',
                           migrate='platform.table'
)

Platform_Attribute = db.define_table('platform_attribute',
                                     Field('platform_id', 'reference platform'),
                                     Field('attribute_name', 'text'),
                                     Field('attribute_value', 'text', 'text'),
                                     format='%(attribute_name)s_%(attribute_value)s',
                                     migrate='platform_attribute.table'
)

Sample = db.define_table('sample',
                         Field('series_id', 'reference series'),
                         Field('platform_id', 'reference platform'),
                         Field('gsm_name', 'text'),
                         format='%(gsm_name)s',
                         migrate='sample.table'
)

Sample_Attribute = db.define_table('sample_attribute',
                                   Field('sample_id', 'reference sample'),
                                   Field('attribute_name', 'string', 256),
                                   Field('attribute_value', 'text', 'text'),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='sample_attribute.table'
)

Sample_View = db.define_table('sample_view',
                              Field('series_id', 'reference series'),
                              Field('platform_id', 'reference platform'),
                              Field('sample_id', 'reference sample'),
                              Field('sample_biomaterial_provider_ch1', 'text'),
                              Field('sample_biomaterial_provider_ch2', 'text'),
                              Field('sample_channel_count', 'text'),
                              Field('sample_characteristics_ch1', 'text'),
                              Field('sample_characteristics_ch2', 'text'),
                              Field('sample_contact_address', 'text'),
                              Field('sample_contact_city', 'text'),
                              Field('sample_contact_country', 'text'),
                              Field('sample_contact_department', 'text'),
                              Field('sample_contact_email', 'text'),
                              Field('sample_contact_fax', 'text'),
                              Field('sample_contact_institute', 'text'),
                              Field('sample_contact_laboratory', 'text'),
                              Field('sample_contact_name', 'text'),
                              Field('sample_contact_phone', 'text'),
                              Field('sample_contact_state', 'text'),
                              Field('sample_contact_web_link', 'text'),
                              Field('sample_contact_zip_postal_code', 'text'),
                              Field('sample_data_processing', 'text'),
                              Field('sample_data_row_count', 'text'),
                              Field('sample_description', 'text'),
                              Field('sample_extract_protocol_ch1', 'text'),
                              Field('sample_extract_protocol_ch2', 'text'),
                              Field('sample_geo_accession', 'text'),
                              Field('sample_growth_protocol_ch1', 'text'),
                              Field('sample_growth_protocol_ch2', 'text'),
                              Field('sample_hyb_protocol', 'text'),
                              Field('sample_instrument_model', 'text'),
                              Field('sample_label_ch1', 'text'),
                              Field('sample_label_ch2', 'text'),
                              Field('sample_label_protocol_ch1', 'text'),
                              Field('sample_label_protocol_ch2', 'text'),
                              Field('sample_last_update_date', 'text'),
                              Field('sample_library_selection', 'text'),
                              Field('sample_library_source', 'text'),
                              Field('sample_library_strategy', 'text'),
                              Field('sample_molecule_ch1', 'text'),
                              Field('sample_molecule_ch2', 'text'),
                              Field('sample_organism_ch1', 'text'),
                              Field('sample_organism_ch2', 'text'),
                              Field('sample_platform_id', 'text'),
                              Field('sample_relation', 'text'),
                              Field('sample_scan_protocol', 'text'),
                              Field('sample_source_name_ch1', 'text'),
                              Field('sample_source_name_ch2', 'text'),
                              Field('sample_status', 'text'),
                              Field('sample_submission_date', 'text'),
                              Field('sample_supplementary_file', 'text'),
                              Field('sample_supplementary_file_1', 'text'),
                              Field('sample_supplementary_file_2', 'text'),
                              Field('sample_supplementary_file_3', 'text'),
                              Field('sample_supplementary_file_4', 'text'),
                              Field('sample_supplementary_file_5', 'text'),
                              Field('sample_taxid_ch1', 'text'),
                              Field('sample_taxid_ch2', 'text'),
                              Field('sample_treatment_protocol_ch1', 'text'),
                              Field('sample_treatment_protocol_ch2', 'text'),
                              Field('sample_type', 'text'),
                              migrate=False)

Sample_Filter = db.define_table('sample_filter',
                                Field('sample_id', 'reference sample'),
                                migrate='sample_filter.table')

Query = db.define_table('query',
                        Field('id', 'id'),
                        Field('sample_contact_name'),
                        migrate=False)
