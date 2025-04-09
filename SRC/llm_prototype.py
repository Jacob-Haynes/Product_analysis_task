import os
import openai
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# Load your OpenAI API key from an environment variable for security
openai.api_key = os.getenv("OPENAI_API_KEY")


def load_prompt_from_file(filepath):
    """Loads the prompt instructions from a text file."""
    try:
        with open(filepath, "r") as f:
            prompt = f.read()
        return prompt
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None


def get_user_input():
    """Simulates receiving user input."""
    user_text = input("Simulated User Input: ")
    return user_text


def call_llm(prompt):
    """Calls the OpenAI API with the given prompt."""
    try:
        client = openai.OpenAI()  # Initialise the OpenAI client
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant.",
                },  # Optional base system message
                {"role": "user", "content": prompt},
            ],
        )
        return response.choices[0].message.content
    except openai.APIError as e:
        print(f"Error calling OpenAI API: {e}")
        return None


if __name__ == "__main__":
    prompt_file = "prompt_instructions.txt"
    instructions = load_prompt_from_file(prompt_file)

    if instructions:
        user_query = get_user_input()
        if user_query is not None:
            full_prompt = f"{instructions}\n\nUser Query: {user_query}\n\nResponse:"
            llm_response = call_llm(full_prompt)

            if llm_response:
                print("\nLLM Response:")
                print(llm_response)
