from FilesProcessing import InputData

def main():
    catalog = InputData('d', '/Users/Maria/PyCharmProjects/helper/__sql_for_es.json')


    # json_content = catalog.files2json()
    # for item in json_content:
    #     params_list = ['CDS', 'primary_report_number']
    #     print catalog.parameterSearch(item, params_list)

if  __name__ =='__main__':
    main()