// Azure Function - Get Product by ID HTTP Trigger

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

// Initialize repository
const productRepo: ProductRepo = new CosmosProductRepo(cosmosOptions);

/**
 * Response format for single product API
 */
interface GetProductResponse {
  readonly success: boolean;
  readonly data?: Product;
  readonly error?: {
    readonly code: string;
    readonly message: string;
  };
}

/**
 * Azure Function to get a product by ID
 *
 * GET /api/products/{id}
 *
 * Returns a single product by its ID
 */
export async function getProductByIdHttp(
  request: HttpRequest,
  context: InvocationContext
): Promise<HttpResponseInit> {
  const productId = request.params.id;

  context.log(
    `HTTP trigger function processed a request to get product: ${productId}`
  );

  // Validate product ID parameter
  if (!productId || productId.trim().length === 0) {
    const errorResponse: GetProductResponse = {
      success: false,
      error: {
        code: 'INVALID_INPUT',
        message: 'Product ID is required',
      },
    };

    return {
      status: 400,
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(errorResponse, null, 2),
    };
  }

  try {
    // Get product from repository
    const result = await productRepo.get(productId.trim());

    if (result.success) {
      const response: GetProductResponse = {
        success: true,
        data: result.data,
      };

      return {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=300', // Cache for 5 minutes
        },
        body: JSON.stringify(response, null, 2),
      };
    }

    // Handle repository errors - result.success is false, so error exists
    const error = (result as { success: false; error: any }).error;
    const statusCode = error.code === 'NOT_FOUND' ? 404 : 500;

    const errorResponse: GetProductResponse = {
      success: false,
      error: {
        code: error.code,
        message: error.message,
      },
    };

    return {
      status: statusCode,
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(errorResponse, null, 2),
    };
  } catch (error: any) {
    context.log('Error getting product:', error);

    const errorResponse: GetProductResponse = {
      success: false,
      error: {
        code: 'INTERNAL_ERROR',
        message: 'An unexpected error occurred while retrieving the product',
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
app.http('getProductById', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'products/{id}',
  handler: getProductByIdHttp,
});
