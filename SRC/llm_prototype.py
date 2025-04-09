import os
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the Gemini API key from the environment variables
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")


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
    """Simulates receiving user text input."""
    user_text = input("Simulated User Input: ")
    return user_text


def call_gemini(prompt):
    """Calls the Gemini API with the given prompt."""
    if not GOOGLE_API_KEY:
        print("Error: GOOGLE_API_KEY environment variable not set.")
        return None

    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel("gemini-2.0-flash")

    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return None


if __name__ == "__main__":
    prompt_file = "prompt_instructions.txt"
    instructions = load_prompt_from_file(prompt_file)

    if instructions:
        user_query = get_user_input()
        if user_query is not None:
            full_prompt = f"{instructions}\n\nUser Query: {user_query}\n\nResponse:"
            gemini_response = call_gemini(full_prompt)

            if gemini_response:
                print("\nGemini Response:")
                print(gemini_response)
