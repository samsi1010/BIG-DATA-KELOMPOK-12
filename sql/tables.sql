-- ============================================================================
-- TAHAP 4.3.1: DATABASE SCHEMA UNTUK PENYIMPANAN DATA HASIL PROCESSING
-- ============================================================================

-- Tabel utama untuk menyimpan seluruh data transaksi retail yang sudah diproses
CREATE TABLE IF NOT EXISTS public.retail_transactions (
    id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity DOUBLE PRECISION,
    invoice_date TIMESTAMP,
    unit_price DOUBLE PRECISION,
    customer_id VARCHAR(50),
    country VARCHAR(100),
    total_price DOUBLE PRECISION,
    
    -- Fitur waktu dari feature engineering
    hour DOUBLE PRECISION,
    day DOUBLE PRECISION,
    month_num DOUBLE PRECISION,
    weekday_num DOUBLE PRECISION,
    month VARCHAR(10),
    
    -- Revenue category
    revenue_category VARCHAR(20),
    
    -- Country encoding
    country_encoded DOUBLE PRECISION,
    
    -- Product features
    product_frequency DOUBLE PRECISION,
    
    -- Customer features
    avg_customer_spend DOUBLE PRECISION,
    customer_frequency DOUBLE PRECISION,
    customer_total_qty DOUBLE PRECISION,
    customer_price_std DOUBLE PRECISION,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index untuk akses cepat
CREATE INDEX IF NOT EXISTS idx_customer_id ON public.retail_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_invoice_date ON public.retail_transactions(invoice_date);
CREATE INDEX IF NOT EXISTS idx_country ON public.retail_transactions(country);
CREATE INDEX IF NOT EXISTS idx_revenue_category ON public.retail_transactions(revenue_category);