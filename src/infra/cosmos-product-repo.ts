// Cosmos DB Product Repository Implementation - Infrastructure Layer

import { CosmosClient, Container, ItemResponse } from '@azure/cosmos';
import { Product } from '../domain/product';
import {
  ProductRepo,
  RepositoryResult,
  RepositoryError,
} from '../domain/product-repo';

/**
 * Configuration options for Cosmos DB connection
 */
export interface CosmosProductRepoOptions {
  readonly endpoint: string;
  readonly databaseId: string;
  readonly containerId: string;
  readonly key?: string; // Optional access key for key-based auth
}

/**
 * Internal DTO type representing the document structure in Cosmos DB
 * Separate from the domain model to allow for storage-specific concerns
 */
interface ProductDocument {
  readonly id: string;
  readonly name: string;
  readonly brand: string;
  readonly category: string;
  readonly model: string;
  readonly processor: string;
  readonly ram: string;
  readonly storage: string;
  readonly gpu: string;
  readonly display: string;
  readonly os: string;
  readonly batteryLife: string;
  readonly weight: string;
  readonly ports: string[];
  readonly connectivity: string[];
  readonly description?: string;
  readonly imageUrl?: string;
  readonly price: number;
  readonly inStock: boolean;
  readonly createdAt: Date;
}

/**
 * Azure Cosmos DB implementation of ProductRepo
 *
 * This infrastructure implementation handles the persistence of Product domain objects
 * in Azure Cosmos DB, including data transformation and error handling.
 */
export class CosmosProductRepo implements ProductRepo {
  private readonly container: Container;

  constructor(options: CosmosProductRepoOptions) {
    // Initialize Cosmos client with appropriate authentication
    const cosmosClient = options.key
      ? new CosmosClient({
          endpoint: options.endpoint,
          key: options.key,
        })
      : new CosmosClient({
          endpoint: options.endpoint,
        }); // Uses default credential chain when no key provided

    this.container = cosmosClient
      .database(options.databaseId)
      .container(options.containerId);
  }

  async list(): Promise<RepositoryResult<Product[]>> {
    try {
      const query = 'SELECT * FROM c';
      const { resources } = await this.container.items
        .query<ProductDocument>(query)
        .fetchAll();

      const products = resources.map((doc) => this.toDomain(doc));
      return { success: true, data: products };
    } catch (error: any) {
      return { success: false, error: this.mapCosmosError(error) };
    }
  }

  /**
   * Converts domain Product to Cosmos DB document format
   */
  private toDocument(product: Product): ProductDocument {
    return {
      id: product.id,
      name: product.name,
      brand: product.brand,
      category: product.category,
      model: product.model,
      processor: product.processor,
      ram: product.ram,
      storage: product.storage,
      gpu: product.gpu,
      display: product.display,
      os: product.os,
      batteryLife: product.batteryLife,
      weight: product.weight,
      ports: product.ports,
      connectivity: product.connectivity,
      description: product.description,
      imageUrl: product.imageUrl,
      price: product.price,
      inStock: product.inStock,
      createdAt: product.createdAt,
    };
  }

  /**
   * Converts Cosmos DB document to domain Product
   */
  private toDomain(document: ProductDocument): Product {
    return {
      id: document.id,
      name: document.name,
      brand: document.brand,
      category: document.category,
      model: document.model,
      processor: document.processor,
      ram: document.ram,
      storage: document.storage,
      gpu: document.gpu,
      display: document.display,
      os: document.os,
      batteryLife: document.batteryLife,
      weight: document.weight,
      ports: document.ports,
      connectivity: document.connectivity,
      description: document.description,
      imageUrl: document.imageUrl,
      price: document.price,
      inStock: document.inStock,
      createdAt: new Date(document.createdAt),
    };
  }

  /**
   * Maps Cosmos DB errors to domain repository errors
   */
  private mapCosmosError(error: any): RepositoryError {
    if (error.code === 409) {
      return {
        code: 'ALREADY_EXISTS',
        message: 'A product with this ID already exists',
      };
    }

    if (error.code === 404) {
      return {
        code: 'NOT_FOUND',
        message: 'Product not found',
      };
    }

    if (error.code >= 400 && error.code < 500) {
      return {
        code: 'VALIDATION_ERROR',
        message: error.message || 'Invalid request',
      };
    }

    return {
      code: 'PERSISTENCE_ERROR',
      message:
        error.message || 'An error occurred while accessing the database',
    };
  }

  /**
   * Creates a new product in Cosmos DB
   */
  async create(product: Product): Promise<RepositoryResult<Product>> {
    try {
      const document = this.toDocument(product);

      const response: ItemResponse<ProductDocument> =
        await this.container.items.create(document, {
          disableAutomaticIdGeneration: true,
        });

      if (response.resource) {
        const createdProduct = this.toDomain(response.resource);
        return { success: true, data: createdProduct };
      }

      return {
        success: false,
        error: {
          code: 'PERSISTENCE_ERROR',
          message: 'Failed to create product - no resource returned',
        },
      };
    } catch (error: any) {
      return {
        success: false,
        error: this.mapCosmosError(error),
      };
    }
  }

  /**
   * Retrieves a product by ID from Cosmos DB
   */
  async get(id: string): Promise<RepositoryResult<Product>> {
    try {
      const response: ItemResponse<ProductDocument> = await this.container
        .item(id, id)
        .read();

      if (response.resource) {
        const product = this.toDomain(response.resource);
        return { success: true, data: product };
      }

      return {
        success: false,
        error: {
          code: 'NOT_FOUND',
          message: `Product with ID '${id}' not found`,
        },
      };
    } catch (error: any) {
      return {
        success: false,
        error: this.mapCosmosError(error),
      };
    }
  }
}
