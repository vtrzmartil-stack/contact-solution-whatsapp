import { createClient } from '@supabase/supabase-js'

// No Vite, acessamos as variáveis usando import.meta.env
const supabaseUrl = import.meta.env.VITE_SUPABASE_URL
const supabaseKey = import.meta.env.VITE_SUPABASE_ANON_KEY

// Cria e exporta a conexão para usar em qualquer lugar do site
export const supabase = createClient(supabaseUrl, supabaseKey)