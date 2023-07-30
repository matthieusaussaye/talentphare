class consumer:
    def __init__(self,
                 consumer_dict : dict) :
        self.name = consumer_dict['name']
        self.age = consumer_dict['age']
        self.prompt_information = consumer_dict['prompt_information']
        self.related_products = consumer_dict['related_products']
        self.consumer_answers = {}
    def save_consumer(self):
        "this function should save the consumer information to azure table"
        return
    def save_answer(self):
        "this function should save the consumer answer to azure table via an upsert"
        return
    def remove_line(self):
        "this function should remove the row given question_i and user_key"
        return







