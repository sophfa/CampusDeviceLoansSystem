import { CosmosClient } from '@azure/cosmos';
import 'dotenv/config';

const endpoint = process.env.COSMOS_ENDPOINT!;
const key = process.env.COSMOS_KEY!;
const client = new CosmosClient({ endpoint, key });

const databaseId = 'catalogue-db';
const containerId = 'Devices';

const products = [
  {
    id: 'PROD-001',
    name: 'ASUS Chromebook 15 CX1505CKA',
    brand: 'ASUS',
    category: 'Laptop',
    model: 'CX1505CKA-EJ0034',
    processor: 'Intel Celeron N4500 (2 cores, up to 2.8GHz)',
    ram: '4 GB LPDDR4',
    storage: '64 GB eMMC',
    gpu: 'Intel UHD Graphics',
    display: '15.6" Full HD (1920x1080)',
    os: 'Chrome OS',
    batteryLife: 'Up to 10 hours',
    weight: '1.8 kg',
    ports: ['USB-C', 'USB-A', 'microSD', 'Headphone Jack'],
    connectivity: ['Wi-Fi 5', 'Bluetooth 4.2'],
    description:
      'Lightweight Chromebook with full HD display, ideal for study and browsing.',
    imageUrl: 'asus.jpg',
    price: 229.99,
    inStock: true,
    createdAt: '2025-10-27T12:00:00Z',
  },
  {
    id: 'PROD-002',
    name: 'Dell Latitude 5400',
    brand: 'Dell',
    category: 'Laptop',
    model: 'Latitude 5400 i5-8365U',
    processor: 'Intel Core i5-8365U (Quad-core, 1.6GHz up to 4.1GHz)',
    ram: '16 GB DDR4',
    storage: '512 GB SSD',
    gpu: 'Intel UHD Graphics 620',
    display: '14" Full HD (1920x1080)',
    os: 'Windows 11 Pro',
    batteryLife: 'Up to 13 hours',
    weight: '1.6 kg',
    ports: ['USB-C', 'USB 3.1', 'HDMI', 'Ethernet', 'Audio Jack', 'microSD'],
    connectivity: ['Wi-Fi 6', 'Bluetooth 5.0'],
    description:
      'Reliable business laptop, renewed edition with fast SSD storage and Windows 11 Pro.',
    imageUrl: 'dell.jpg',
    price: 479.0,
    inStock: false,
    createdAt: '2025-10-27T12:05:00Z',
  },
  {
    id: 'PROD-003',
    name: 'HP Stream 14s-dq0000sa',
    brand: 'HP',
    category: 'Laptop',
    model: '14s-dq0000sa',
    processor: 'Intel Celeron N4120 (Quad-core, 1.1GHz up to 2.6GHz)',
    ram: '4 GB DDR4',
    storage: '64 GB eMMC',
    gpu: 'Intel UHD Graphics 600',
    display: '14" HD (1366x768)',
    os: 'Windows 11 Home S Mode',
    batteryLife: 'Up to 9 hours',
    weight: '1.47 kg',
    ports: ['USB-C', 'USB 3.1', 'HDMI', 'Headphone Jack'],
    connectivity: ['Wi-Fi 5', 'Bluetooth 4.2'],
    description:
      'Affordable laptop for students with free 12-month Microsoft 365 subscription.',
    imageUrl: 'hp.jpg',
    price: 199.0,
    inStock: true,
    createdAt: '2025-10-27T12:10:00Z',
  },
  {
    id: 'PROD-004',
    name: 'Lenovo V15 ADA',
    brand: 'Lenovo',
    category: 'Laptop',
    model: '82C7000BUK',
    processor: 'AMD Athlon Silver 3150U (Dual-core, up to 3.3GHz)',
    ram: '8 GB DDR4',
    storage: '256 GB SSD',
    gpu: 'AMD Radeon Graphics',
    display: '15.6" Full HD (1920x1080)',
    os: 'Windows 10 Home',
    batteryLife: 'Up to 7.5 hours',
    weight: '1.85 kg',
    ports: ['USB 3.1', 'USB 2.0', 'HDMI', 'Audio Jack'],
    connectivity: ['Wi-Fi 5', 'Bluetooth 5.0'],
    description:
      'Full HD laptop offering solid performance for everyday work and study tasks.',
    imageUrl: 'lenovo.jpg',
    price: 299.0,
    inStock: true,
    createdAt: '2025-10-27T12:15:00Z',
  },
];

async function seed() {
  const container = client.database(databaseId).container(containerId);

  await container.items
    .query('SELECT * FROM c')
    .fetchAll()
    .then(async (res) => {
      for (const item of res.resources) {
        await container.item(item.id, item.id).delete();
      }
    });

  for (const product of products) {
    const { resource } = await container.items.create(product);
  }
}

seed().catch(console.error);
