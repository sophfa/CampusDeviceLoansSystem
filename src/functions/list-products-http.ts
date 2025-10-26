// Azure Function - List Products HTTP Trigger

import {
  app,
  HttpRequest,
  HttpResponseInit,
  InvocationContext,
} from '@azure/functions';
import { Product } from '../domain/product';
import { ProductRepo } from '../domain/product-repo';
import { CosmosProductRepo } from '../infra/cosmos-product-repo';

// Configuration from environment variables
const cosmosOptions = {
  endpoint: process.env.COSMOS_ENDPOINT || 'https://localhost:8081',
  databaseId: process.env.COSMOS_DATABASE || 'ProductsDB',
  containerId: process.env.COSMOS_CONTAINER || 'Products',
  key: process.env.COSMOS_KEY,
};

// Initialize repository - in production, this could be dependency injected
const productRepo: ProductRepo = new CosmosProductRepo(cosmosOptions);

/**
 * Response format for product list API
 */
interface ListProductsResponse {
  readonly success: boolean;
  readonly data?: Product[];
  readonly error?: {
    readonly code: string;
    readonly message: string;
  };
  readonly metadata?: {
    readonly count: number;
    readonly timestamp: string;
  };
}

/**
 * Azure Function to list all products
 *
 * GET /api/products
 *
 * Returns a list of all products in the system
 */
export async function listProductsHttp(
  request: HttpRequest,
  context: InvocationContext
): Promise<HttpResponseInit> {
  context.log('HTTP trigger function processed a request to list products');

  try {
    // Note: The current ProductRepo interface only has get() and create() methods
    // For a real implementation, we'd need to add a list() or getAll() method
    // For now, we'll return a mock response structure

    // TODO: Add list() method to ProductRepo interface and implementation
    // const result = await productRepo.list();

    // Mock response for demonstration
    const mockProducts: Product[] = [
      {
        id: 'PROD-001',
        name: 'Sample Product 1',
        price: 99.99,
        category: 'Electronics',
        description: 'A sample product for demonstration',
        inStock: true,
        createdAt: new Date(),
      },
      {
        id: 'PROD-002',
        name: 'Sample Product 2',
        price: 49.99,
        category: 'Accessories',
        inStock: false,
        createdAt: new Date(),
      },
    ];

    const response: ListProductsResponse = {
      success: true,
      data: mockProducts,
      metadata: {
        count: mockProducts.length,
        timestamp: new Date().toISOString(),
      },
    };

    return {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
      },
      body: JSON.stringify(response, null, 2),
    };
  } catch (error: any) {
    context.log('Error listing products:', error);

    const errorResponse: ListProductsResponse = {
      success: false,
      error: {
        code: 'INTERNAL_ERROR',
        message: 'An unexpected error occurred while listing products',
      },
    };

    return {
      status: 500,
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(errorResponse, null, 2),
    };
  }
}

// Register the function with Azure Functions runtime
app.http('listProducts', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'products',
  handler: listProductsHttp,
});
