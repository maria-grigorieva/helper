from fileCatalogProcessing import FileCatalog

def main():
    catalog = FileCatalog()
    #print catalog.files2string()
    json_content = catalog.files2json()
    #print next(json_content)
    #print catalog.parameterSearch(next(json_content), params_list)
    for item in json_content:
        params_list = ['CDS', 'primary_report_number']
        print catalog.parameterSearch(item, params_list)
        # print catalog.searchParameterByType(item, params_list, list)
        #print catalog.searchString(item, 'STDM-2010-01')



if  __name__ =='__main__':
    main()