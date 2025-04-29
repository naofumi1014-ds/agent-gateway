import requests
import os
from dotenv import load_dotenv

class Tools:
    def __init__(self):
        self.tools = []
        self.tool_descriptions = []
        self.output_descriptions = []
        self.tool_functions = []
        self.tool_names = []

    def add_tool(self, tool_name, tool_description, output_description, tool_function):
        self.tools.append(tool_name)
        self.tool_descriptions.append(tool_description)
        self.output_descriptions.append(output_description)
        self.tool_functions.append(tool_function)
        self.tool_names.append(tool_name)

    @staticmethod
    def html_crawl(url):
        response = requests.get(url)
        return response.text

    @staticmethod
    def search_weather(location):
        return f"{location}の天気は晴れです。"