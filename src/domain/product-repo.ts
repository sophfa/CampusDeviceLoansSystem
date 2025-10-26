// Product Repository Interface - Pure Domain Contract

import { Product } from './product';

/**
 * Repository error type for domain-level error handling
 */
export interface RepositoryError {
  readonly code:
    | 'NOT_FOUND'
    | 'ALREADY_EXISTS'
    | 'PERSISTENCE_ERROR'
    | 'VALIDATION_ERROR';
  readonly message: string;
  readonly field?: string;
}

/**
 * Result type for repository operations - Either success or error
 */
export type RepositoryResult<T> =
  | { success: true; data: T }
  | { success: false; error: RepositoryError };

/**
 * Pure domain repository interface for Product persistence
 *
 * This interface defines the contract for product persistence operations
 * without any implementation details, keeping it pure domain-focused.
 */
export interface ProductRepo {

  list(): Promise<RepositoryResult<Product[]>>  
  /**
   * Persists a product to the repository
   *
   * @param product - The product to save
   * @returns Promise resolving to either the saved product or a repository error
   */
  create(product: Product): Promise<RepositoryResult<Product>>;

  /**
   * Retrieves a product by its unique identifier
   *
   * @param id - The unique product identifier
   * @returns Promise resolving to either the found product or a repository error
   */
  get(id: string): Promise<RepositoryResult<Product>>;
}
