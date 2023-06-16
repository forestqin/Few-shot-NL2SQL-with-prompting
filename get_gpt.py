#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
import openai
import os
import sys
import requests
import json

openai.api_key = os.getenv("OPENAI_API_KEY")
QIAOJIANG_API = "http://qiaojiang.baidu-int.com/entry/common"
QIAOJIANG_USER = "impl"
QIAOJIANG_PWD = "LZ0fPkRKLjiPL1aK"
HTTP_HEADERS = {'content-type': 'application/json'}

class ChatGPT :
    
    def get_prompt(question):
        tables_summary = f"""
        Table fc_ocpc_cv_core:
            event_day (string)
            cmatch (string)
            rank (string)
            wmatch (string)
            userid (string)
            planid (string)
            unitid (string)
            winfoid (string)
            wordid (string)
            shw_time (string)
            ocpc_stage (string)
            ocpc_type_list (string)
            ovlexp_id_list (string)
            show (string)
            click (string)
            charge (string)
            

        User: how many first stage converts occur last day?
        Assistant: select event_day, count(1) as cnvt_num from fc_ocpc_cv_core where event_day = date_sub(current_date(), 1) and ocpc_stage = 1.
        """

        prompt = f"""{tables_summary}

        Given the above schemas and data, you should act as the middleman between USER and a DATABASE. 
        Your main goal is to answer questions based on data in a Hive database. 
        Make sure that you can write a detailed and correct Hive sql. 
        You do this by executing valid queries against the database and interpreting the results to anser the questions from the USER.
        用中文进行解释
        
        User: "{question}"

        Comment the query with your logic."""
        return prompt

    
    def request_basic_model(user_input_prompt,
                            model="yiyan",
                            debug=False,
                            max_tokens=1000,
                            temperature=0.0):

        # Define the request payload as a dictionary
        payload = {
            "request_type": "aigc_text",
            "username": QIAOJIANG_USER,
            "password": QIAOJIANG_PWD,
            "req": {
                "model": model,  # or yiyan, c_model
                "max_tokens": max_tokens,
                "temperature": temperature,
                "prompt": user_input_prompt
            }
        }

        print(payload) if debug else None

        # Make a POST request to a URL with the payload
        response = requests.post(url=QIAOJIANG_API, headers=HTTP_HEADERS, data=json.dumps(payload))
        return response


    def parse_basic_model_response(http_response):
        # Check if the request was successful (status code 200)
        if http_response.status_code != 200:
            print("Request failed with status code: {}", http_response.status_code)
            print(http_response)
            return None

        # Parse the response JSON
        response_data = json.loads(http_response.text)

        # Parse the "header" field
        if response_data["header"]["failures"]["code"] != 0:
            print("Request failed with header code: {}, msg: {}".format(
                response_data["header"]["failures"]["code"],
                response_data["header"]["failures"]["message"]))
            return None

        # Parse the body JSON
        if response_data["body"]["message"]["ret_code"] != 0:
            print("Request failed with body code: {}, msg: {}".format(
                response_data["body"]["message"]["ret_code"],
                response_data["body"]["message"]["ret_msg"]))
            return None

        return response_data["body"]["message"]["res"]["text"]