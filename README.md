# Blockchain Data Pipelines

Welcome to the Blockchain Data Pipelines repository! This project serves as a practical exploration of data engineering architectures, demonstrating how to process Ethereum blockchain data.

Within this repository, you will find two distinct approaches to building data pipelines. By having both architectures side-by-side, you can easily compare the traditional method with the modern approach.

## Repository Structure

This repository is split into two main directories, each demonstrating a different pipeline design:

### 1. The ETL Pipeline (`/ETL-Data-Pipeline`)
**Extract -> Transform -> Load**

In this approach, data is extracted from the blockchain, transformed externally using a separate processing engine, and finally loaded into the data warehouse for storage.
- **Navigation:** [Explore the ETL Pipeline](./ETL-Data-Pipeline)

### 2. The ELT Pipeline (`/ELT-Data-Pipeline`)
**Extract -> Load -> Transform**

In this modern approach, data is extracted and loaded *directly* into the destination database first. The transformations are then handled natively inside the database itself.
- **Navigation:** [Explore the ELT Pipeline](./ELT-Data-Pipeline)

## Getting Started

To keep this overview simple, all the technical complexities, setup instructions, and deep-dive articles are kept within their respective folders. 

To get started, simply choose a pipeline above, navigate into its folder, and read the dedicated `README.md` to begin your journey!
