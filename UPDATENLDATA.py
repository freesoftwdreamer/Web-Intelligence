from mpi4py import MPI
import pandas as pd
from tqdm import tqdm  # Import tqdm for progress bar
import logging

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_update.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_ecommerce_indicator(df_input, df_probabilities):
    # Merge the datasets on the URL column
    merged_df = pd.merge(df_input, df_probabilities, left_on='url', right_on='URL', how='left')

    # Update the 'ecommerce' field based on the rounded probability
    merged_df['ecommerce'] = merged_df.apply(
        lambda row: 1 if row['Probability (%)'] >= 20 else row['ecommerce'],
        axis=1
    )



    # Drop the 'Probability (%)' and 'URL' columns as they are no longer needed
    merged_df.drop(columns=['Probability (%)', 'URL'], inplace=True)

    return merged_df

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # Read the input CSV file
        df_input = pd.read_csv('NLinput.csv')

        # Read the probabilities CSV file
        df_probabilities = pd.read_csv('ecommerce_probabilities_parallel_NLDATASET.csv')

        # Split the input DataFrame among processes
        chunks = [df_input[i::size] for i in range(size)]
    else:
        df_input = None
        df_probabilities = None
        chunks = None

    # Scatter the input DataFrame chunks to all processes
    df_input_chunk = comm.scatter(chunks, root=0)

    # Broadcast the probabilities DataFrame to all processes
    df_probabilities = comm.bcast(df_probabilities, root=0)

    # Each process updates its chunk of the input DataFrame with a progress bar
    updated_df_chunk = df_input_chunk.copy()  # Create a copy to avoid modifying the original chunk
    for index, row in tqdm(df_input_chunk.iterrows(), total=df_input_chunk.shape[0], desc=f"Processing on rank {rank}"):
        url = row['url']
        probability = df_probabilities.loc[df_probabilities['URL'] == url, 'Probability (%)'].values
        if len(probability) > 0:
            probability = probability[0]
            if probability >= 20:
                updated_df_chunk.at[index, 'ecommerce'] = 1
        if url == 'https://www.hertz.de':
            updated_df_chunk.at[index, 'ecommerce'] = 1
        if url == 'https://www.notebooksbilliger.de':
            updated_df_chunk.at[index, 'ecommerce'] = 1

    # Gather updated chunks from all processes
    updated_df_chunks = comm.gather(updated_df_chunk, root=0)

    if rank == 0:
        # Concatenate all updated chunks into a single DataFrame
        updated_df = pd.concat(updated_df_chunks)

        # Sort the updated DataFrame back to the original order
        updated_df.sort_index(inplace=True)

        # Save the updated DataFrame back to DEinput.csv
        updated_df.to_csv('NEWNLinput.csv', index=False)

        logging.info("Updated input saved to 'ATinput.csv'.")

if __name__ == "__main__":
    main()
