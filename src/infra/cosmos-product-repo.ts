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
  readonly price: number;
  readonly category: string;
  readonly description?: string;
  readonly inStock: boolean;
  readonly createdAt: string; // ISO string for JSON serialization
  readonly _rid?: string; // Cosmos DB system property
  readonly _self?: string; // Cosmos DB system property
  readonly _etag?: string; // Cosmos DB system property
  readonly _attachments?: string; // Cosmos DB system property
  readonly _ts?: number; // Cosmos DB system property
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

  /**
   * Converts domain Product to Cosmos DB document format
   */
  private toDocument(product: Product): ProductDocument {
    return {
      id: product.id,
      name: product.name,
      price: product.price,
      category: product.category,
      description: product.description,
      inStock: product.inStock,
      createdAt: product.createdAt.toISOString(),
    };
  }

  /**
   * Converts Cosmos DB document to domain Product
   */
  private toDomain(document: ProductDocument): Product {
    return {
      id: document.id,
      name: document.name,
      price: document.price,
      category: document.category,
      description: document.description,
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
