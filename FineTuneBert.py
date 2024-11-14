import torch
from transformers import BertTokenizer, BertForSequenceClassification, Trainer, TrainingArguments
from sklearn.model_selection import train_test_split
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Function to extract text from HTML
def extract_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text()

# Function to fetch URL content with retry logic
def fetch_url_content(url, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching URL {url} (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(delay)
    logging.error(f"Failed to fetch URL {url} after {max_retries} attempts")
    return None

# Load pre-trained multilingual BERT model and tokenizer
model_name = 'bert-base-multilingual-uncased'
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertForSequenceClassification.from_pretrained(model_name, num_labels=2)

# Load your dataset
df = pd.read_csv('classified_urls_DE_corrected.csv')

# Filter the dataset to include only rows where the ecommerce=1
df = df[df['ecommerce'] == 1]

# Validate the dataset
if df.empty:
    logging.error("The filtered dataset is empty. Please check the dataset and filtering logic.")
    exit(1)

# Fetch URL content in parallel with retry logic
start_time = time.time()
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_url_content, url): url for url in df['url']}
    for future in as_completed(futures):
        url = futures[future]
        try:
            df.loc[df['url'] == url, 'html_content'] = future.result()
        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")
print(f"Fetched URL content in {time.time() - start_time} seconds")

# Extract text from HTML content
df['text'] = df['html_content'].apply(extract_text)

# Use "ecommerce" as the flag for e-commerce
df['ecommerce'] = df['ecommerce'].apply(lambda x: 1 if x else 0)

# Split the dataset into training and validation sets
train_texts, val_texts, train_labels, val_labels = train_test_split(df['text'], df['ecommerce'], test_size=0.2)

# Tokenize the texts
train_encodings = tokenizer(train_texts.tolist(), truncation=True, padding=True, max_length=512)
val_encodings = tokenizer(val_texts.tolist(), truncation=True, padding=True, max_length=512)

# Convert labels to tensors
train_labels = torch.tensor(train_labels.tolist())
val_labels = torch.tensor(val_labels.tolist())

# Create a PyTorch dataset
class EcommerceDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = self.labels[idx]
        return item

    def __len__(self):
        return len(self.labels)

train_dataset = EcommerceDataset(train_encodings, train_labels)
val_dataset = EcommerceDataset(val_encodings, val_labels)

# Define training arguments
training_args = TrainingArguments(
    output_dir='./results',
    num_train_epochs=3,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    warmup_steps=500,
    weight_decay=0.01,
    logging_dir='./logs',
    logging_steps=10,
    no_cuda=False  # Ensure training runs on GPU
)

# Initialize the Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset
)

# Fine-tune the model
trainer.train()

# Save the fine-tuned model
model.save_pretrained('./fine_tuned_model')
tokenizer.save_pretrained('./fine_tuned_model')
