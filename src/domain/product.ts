// Product Domain Model - Pure Functional Approach

/**
 * Product value object representing an immutable product entity
 */
export interface Product {
  readonly id: string;
  readonly name: string;
  readonly price: number;
  readonly category: string;
  readonly description?: string;
  readonly inStock: boolean;
  readonly createdAt: Date;
}

/**
 * Input parameters for creating a Product
 */
export interface CreateProductParams {
  readonly id: string;
  readonly name: string;
  readonly price: number;
  readonly category: string;
  readonly description?: string;
  readonly inStock?: boolean;
}

/**
 * Validation error type for product creation
 */
export interface ProductValidationError {
  readonly field: string;
  readonly message: string;
}

/**
 * Result type for product creation - Either success or validation errors
 */
export type ProductCreationResult =
  | { success: true; product: Product }
  | { success: false; errors: ProductValidationError[] };

/**
 * Validates a product name
 */
const validateName = (name: string): ProductValidationError[] => {
  const errors: ProductValidationError[] = [];

  if (!name || name.trim().length === 0) {
    errors.push({ field: 'name', message: 'Product name is required' });
  } else if (name.trim().length < 2) {
    errors.push({
      field: 'name',
      message: 'Product name must be at least 2 characters long',
    });
  } else if (name.trim().length > 100) {
    errors.push({
      field: 'name',
      message: 'Product name cannot exceed 100 characters',
    });
  }

  return errors;
};

/**
 * Validates a product price
 */
const validatePrice = (price: number): ProductValidationError[] => {
  const errors: ProductValidationError[] = [];

  if (typeof price !== 'number' || isNaN(price)) {
    errors.push({ field: 'price', message: 'Price must be a valid number' });
  } else if (price < 0) {
    errors.push({ field: 'price', message: 'Price cannot be negative' });
  } else if (price > 1000000) {
    errors.push({ field: 'price', message: 'Price cannot exceed 1,000,000' });
  }

  return errors;
};

/**
 * Validates a product ID
 */
const validateId = (id: string): ProductValidationError[] => {
  const errors: ProductValidationError[] = [];

  if (!id || id.trim().length === 0) {
    errors.push({ field: 'id', message: 'Product ID is required' });
  } else if (!/^[a-zA-Z0-9-_]+$/.test(id.trim())) {
    errors.push({
      field: 'id',
      message:
        'Product ID can only contain alphanumeric characters, hyphens, and underscores',
    });
  }

  return errors;
};

/**
 * Validates a product category
 */
const validateCategory = (category: string): ProductValidationError[] => {
  const errors: ProductValidationError[] = [];

  if (!category || category.trim().length === 0) {
    errors.push({ field: 'category', message: 'Product category is required' });
  } else if (category.trim().length < 2) {
    errors.push({
      field: 'category',
      message: 'Category must be at least 2 characters long',
    });
  }

  return errors;
};

/**
 * Validates optional description
 */
const validateDescription = (
  description?: string
): ProductValidationError[] => {
  const errors: ProductValidationError[] = [];

  if (description && description.length > 500) {
    errors.push({
      field: 'description',
      message: 'Description cannot exceed 500 characters',
    });
  }

  return errors;
};

/**
 * Combines all validation results
 */
const validateProduct = (
  params: CreateProductParams
): ProductValidationError[] => {
  return [
    ...validateId(params.id),
    ...validateName(params.name),
    ...validatePrice(params.price),
    ...validateCategory(params.category),
    ...validateDescription(params.description),
  ];
};

/**
 * Factory function to create a Product with validation
 * Returns either a valid Product or validation errors
 */
export const createProduct = (
  params: CreateProductParams
): ProductCreationResult => {
  const errors = validateProduct(params);

  if (errors.length > 0) {
    return { success: false, errors };
  }

  const product: Product = {
    id: params.id.trim(),
    name: params.name.trim(),
    price: params.price,
    category: params.category.trim(),
    description: params.description?.trim(),
    inStock: params.inStock ?? true,
    createdAt: new Date(),
  };

  return { success: true, product };
};

/**
 * Pure function to update product price
 */
export const updateProductPrice = (
  product: Product,
  newPrice: number
): ProductCreationResult => {
  const priceErrors = validatePrice(newPrice);

  if (priceErrors.length > 0) {
    return { success: false, errors: priceErrors };
  }

  const updatedProduct: Product = {
    ...product,
    price: newPrice,
  };

  return { success: true, product: updatedProduct };
};

/**
 * Pure function to update product stock status
 */
export const updateProductStock = (
  product: Product,
  inStock: boolean
): Product => {
  return {
    ...product,
    inStock,
  };
};

/**
 * Pure function to check if product is available
 */
export const isProductAvailable = (product: Product): boolean => {
  return product.inStock && product.price > 0;
};

/**
 * Pure function to format product for display
 */
export const formatProductDisplay = (product: Product): string => {
  const stockStatus = product.inStock ? 'In Stock' : 'Out of Stock';
  const price = product.price.toFixed(2);

  return `${product.name} - $${price} (${stockStatus})`;
};

/**
 * Pure function to calculate discounted price
 */
export const calculateDiscountedPrice = (
  product: Product,
  discountPercent: number
): number => {
  if (discountPercent < 0 || discountPercent > 100) {
    throw new Error('Discount percentage must be between 0 and 100');
  }

  return product.price * (1 - discountPercent / 100);
};
