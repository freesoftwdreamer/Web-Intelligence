from llama_cpp import Llama

# Load the model
model_path = "/home/corradin/mymodels/Meta-Llama-3.1-8B-Instruct-128k-Q4_0.gguf"

llm = Llama(model_path=model_path, n_ctx=512)

# Define a prompt for text generation
prompt = "What are the benefits of renewable energy?"
print("Question:",prompt)
# Generate a response
response = llm(prompt, max_tokens=100, temperature=0.7)

# Extract and print the generated text
generated_text = response["choices"][0]["text"].strip()
print("Generated Response:", generated_text)
