import pandas as pd
import time
import os
import sys
from get_gpt import ChatGPT as gpt
from get_gpt import schema_linking_prompt

os.chdir(sys.path[0])

def test_1():
    model = "c_model"
    question = "how many converts occur last day?"
    question = "calculate total of shows, clicks and ctr(formula is clicks devide by shows)from 20230301 to 20230307 group by day and cmatch, and then sorted by ctr descending"
    question = "请计算过去一周的展现量，点击量，以及点击率（=点击量/展现量）,并按cmatch和day分组聚合，最终按ctr2进行降序排列"
    prompt = gpt.get_prompt(question)
    response = gpt.request_basic_model(prompt, model, debug=False)
    answer = gpt.parse_basic_model_response(response)
    print(f"{question=}")
    print(answer)


def load_data(DATASET):
    return pd.read_json(DATASET)

def creatiing_schema(DATASET_JSON):
    schema_df = pd.read_json(DATASET_JSON)
    schema_df = schema_df.drop(['column_names','table_names'], axis=1)
    schema = []
    f_keys = []
    p_keys = []
    for index, row in schema_df.iterrows():
        tables = row['table_names_original']
        col_names = row['column_names_original']
        col_types = row['column_types']
        foreign_keys = row['foreign_keys']
        primary_keys = row['primary_keys']
        for col, col_type in zip(col_names, col_types):
            index, col_name = col
            if index == -1:
                for table in tables:
                    schema.append([row['db_id'], table, '*', 'text'])
            else:
                schema.append([row['db_id'], tables[index], col_name, col_type])
        for primary_key in primary_keys:
            index, column = col_names[primary_key]
            p_keys.append([row['db_id'], tables[index], column])
        for foreign_key in foreign_keys:
            first, second = foreign_key
            first_index, first_column = col_names[first]
            second_index, second_column = col_names[second]
            f_keys.append([row['db_id'], tables[first_index], tables[second_index], first_column, second_column])
    spider_schema = pd.DataFrame(schema, columns=['Database name', ' Table Name', ' Field Name', ' Type'])
    spider_primary = pd.DataFrame(p_keys, columns=['Database name', 'Table Name', 'Primary Key'])
    spider_foreign = pd.DataFrame(f_keys,
                        columns=['Database name', 'First Table Name', 'Second Table Name', 'First Table Foreign Key',
                                 'Second Table Foreign Key'])
    return spider_schema, spider_primary, spider_foreign

def schema_linking_prompt_maker(test_sample_text, database):
    instruction = "# Find the schema_links for generating SQL queries for each question based on the database schema and Foreign keys.\n"
    fields = find_fields_MYSQL_like(database)
    foreign_keys = "Foreign_keys = " + find_foreign_keys_MYSQL_like(database) + '\n'
    prompt = instruction + schema_linking_prompt + fields + foreign_keys + 'Q: "' + test_sample_text + """"\nA: Let’s think step by step."""
    return prompt


def find_foreign_keys_MYSQL_like(db_name):
    df = spider_foreign[spider_foreign['Database name'] == db_name]
    output = "["
    for index, row in df.iterrows():
        output += row['First Table Name'] + '.' + row['First Table Foreign Key'] + " = " + row['Second Table Name'] + '.' + row['Second Table Foreign Key'] + ','
    output= output[:-1] + "]"
    return output


def find_fields_MYSQL_like(db_name):
    df = spider_schema[spider_schema['Database name'] == db_name]
    df = df.groupby(' Table Name')
    output = ""
    for name, group in df:
        output += "Table " + name + ', columns = ['
        for index, row in group.iterrows():
            output += row[" Field Name"]+','
        output = output[:-1]
        output += "]\n"
    return output


if __name__ == '__main__':
    # test_1()
    DATASET_SCHEMA = "./data/spider/tables.json"
    DATASET = "./data/spider/dev.json"
    OUTPUT_FILE = "./output/qt_predicted_sql.txt"
    model = "c_model"
    spider_schema, spider_primary, spider_foreign = creatiing_schema(DATASET_SCHEMA)
    val_df = load_data(DATASET)
    print(f"Number of data samples {val_df.shape[0]}")
    CODEX = []
    count = 0
    for index, row in val_df.iterrows():
        #if index < 405: continue #for testing
        print(f"index is {index}")
        print(row['query'])
        print(row['question'])
        schema_links = None
        while schema_links is None:
            try:
                s_promt = schema_linking_prompt_maker(row['question'], row['db_id'])
                # schema_links = GPT4_generation(s_promt)
                # prompt = gpt.get_prompt(s_promt)
                response = gpt.request_basic_model(s_promt, model, debug=False)
                schema_links = gpt.parse_basic_model_response(response)
            except:
                # time.sleep(3)
                pass
        try:
            schema_links = schema_links.split("Schema_links: ")[1]
        except:
            print("Slicing error for the schema_linking module")
            schema_links = "[]"
        print(schema_links)
        count += 1
        if count == 5: 
            break
    

