class business_product:
    def __init__(self,
                 business_dict : dict) :
        self.product_name = business_dict['product_name']
        self.product_type = business_dict['product_type']
        self.question_nb = business_dict['nb_questions']
        self.precisions = business_dict['precisions']
        self.prompt_engineering = business_dict['prompt_engineering']
        self.survey_type = business_dict['survey_type']

    def create_db(self) :
        '''this function should create an azure DB related to a product'''
        return

    def create_tables(self) :
        '''this function should create the tables related to the azure db
        - table answer (user_key|question_i|question_text|answer)
        - table user (user_name|user_key|age|prompt_information)
        '''
        return






