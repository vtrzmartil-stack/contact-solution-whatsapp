import { useState, useEffect } from 'react';
import './App.css';

interface Lead {
  id: string;
  nome?: string;
  telefone: string;
  status: 'bot' | 'humano';
  fase: number;
}

interface UserSession {
  companyId: string;
  companyName: string;
  role: 'admin' | 'client';
}

function App() {
  // --- ESTADOS DE NAVEGAÇÃO E SESSÃO ---
  const [currentView, setCurrentView] = useState<'login' | 'register' | 'dashboard'>('login');
  const [session, setSession] = useState<UserSession | null>(null);
  const [activeTab, setActiveTab] = useState('leads');
  
  // --- ESTADOS DE DADOS ---
  const [leads, setLeads] = useState<Lead[]>([]);
  const [loading, setLoading] = useState(false);

  // URL DO SEU BACKEND
  const API_URL = "https://contact-solution-whatsapp.onrender.com";

  // ==========================================
  // FUNÇÕES PRONTAS PARA O BACKEND
  // ==========================================

  // 1. Função de Login
  const handleLogin = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const email = formData.get('email');
    const password = formData.get('password');

    setLoading(true);
    try {
      // SIMULAÇÃO DE LOGIN (Substituiremos pelo fetch real depois)
      if (email === 'admin@solution.com' && password === '123') {
        setSession({ companyId: 'MASTER', companyName: 'Solution Admin', role: 'admin' });
        // Injetando dados falsos para visualização
        setLeads([
          { id: '1', nome: 'Vitor Hugo', telefone: '+55 11 9999-0001', status: 'bot', fase: 3 },
          { id: '2', nome: 'Empresa Alpha', telefone: '+55 11 9999-0002', status: 'humano', fase: 9 }
        ]);
        setCurrentView('dashboard');
      } else {
        setSession({ companyId: 'CLIENT_01', companyName: 'Empresa Cliente', role: 'client' });
        setLeads([
          { id: '3', nome: 'Lead Genérico', telefone: '+55 11 8888-0000', status: 'bot', fase: 1 }
        ]);
        setCurrentView('dashboard');
      }
    } catch (error) {
      console.error("Erro no login", error);
    } finally {
      setLoading(false);
    }
  };

  // 2. Função de Registro de Nova Empresa
  const handleRegister = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const data = Object.fromEntries(formData.entries());

    // RESOLVE O AVISO "data is declared but never read"
    console.log("Dados prontos para envio ao backend:", data);

    setLoading(true);
    try {
      // Simulação
      setTimeout(() => {
        alert("Empresa registrada com sucesso na base de dados!");
        setCurrentView('login');
        setLoading(false);
      }, 1000);
    } catch (error) {
      console.error(error);
      setLoading(false);
    }
  };

  // 3. Buscar Leads
  const fetchLeads = async () => {
    if (!session) return;
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/leads/${session.companyId}`);
      if (response.ok) {
        const data = await response.json();
        setLeads(Array.isArray(data) ? data : []);
      }
    } catch (error) {
      console.error("Erro ao buscar leads:", error);
    } finally {
      setLoading(false);
    }
  };

  // 4. Salvar as 9 Etapas do Fluxo
  const handleDeployFlow = async () => {
    if (!session) return;
    const textareas = document.querySelectorAll('.step-box textarea') as NodeListOf<HTMLTextAreaElement>;
    const messages = Array.from(textareas).map(txt => txt.value);
    
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/config/flow`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ companyId: session.companyId, flow_messages: messages })
      });
      if (response.ok) alert("Fluxo salvo e atualizado no servidor!");
    } catch (error) {
      alert("Erro ao conectar com o backend.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (currentView === 'dashboard') fetchLeads();
  }, [currentView]);


  // ==========================================
  // RENDERIZAÇÃO DAS TELAS
  // ==========================================

  if (currentView === 'login') {
    return (
      <div className="auth-container">
        <div className="auth-card">
          <div className="auth-title">ContactSolution</div>
          <div className="auth-subtitle">Acesse o painel da sua empresa</div>
          
          <form onSubmit={handleLogin}>
            <input type="email" name="email" placeholder="E-mail corporativo" className="input-field" required />
            <input type="password" name="password" placeholder="Senha" className="input-field" required />
            <button type="submit" className="btn-primary" disabled={loading}>
              {loading ? 'Autenticando...' : 'Entrar no Sistema'}
            </button>
          </form>
          
          <button className="btn-link" onClick={() => setCurrentView('register')}>
            Nova empresa? Registre-se aqui
          </button>
        </div>
      </div>
    );
  }

  if (currentView === 'register') {
    return (
      <div className="auth-container">
        <div className="auth-card">
          <div className="auth-title">Registro de Empresa</div>
          <div className="auth-subtitle">Crie seu banco de dados exclusivo</div>
          
          <form onSubmit={handleRegister}>
            <input type="text" name="companyName" placeholder="Nome da Empresa" className="input-field" required />
            <input type="email" name="email" placeholder="E-mail administrador" className="input-field" required />
            <input type="tel" name="whatsapp" placeholder="WhatsApp do Bot" className="input-field" required />
            <input type="password" name="password" placeholder="Crie uma senha" className="input-field" required />
            
            <button type="submit" className="btn-primary" disabled={loading}>
              {loading ? 'Criando banco de dados...' : 'Registrar Empresa'}
            </button>
          </form>
          
          <button className="btn-link" onClick={() => setCurrentView('login')}>
            Já tem uma conta? Faça login
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="app-layout">
      {/* MENU LATERAL */}
      <aside className="sidebar">
        <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#fff', marginBottom: '10px' }}>ContactSolution</div>
        <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '40px' }}>
          Logado como: <br/><strong style={{color: '#fff'}}>{session?.companyName}</strong>
        </div>

        <nav style={{ flex: 1 }}>
          <div className={`nav-item ${activeTab === 'leads' ? 'active' : ''}`} onClick={() => setActiveTab('leads')}>
            📊 Gestão de Leads
          </div>
          <div className={`nav-item ${activeTab === 'flow' ? 'active' : ''}`} onClick={() => setActiveTab('flow')}>
            ⚙️ Configurar Fluxo (9 Etapas)
          </div>
          
          {session?.role === 'admin' && (
            <div className={`nav-item ${activeTab === 'infra' ? 'active' : ''}`} onClick={() => setActiveTab('infra')}>
              🏢 Infraestrutura Geral
            </div>
          )}
        </nav>

        <button className="btn-link" style={{ textAlign: 'left', color: '#ef4444' }} onClick={() => {setSession(null); setCurrentView('login');}}>
          Sair do Sistema
        </button>
      </aside>

      {/* CONTEÚDO PRINCIPAL */}
      <main className="main-content">
        
        {/* ABA: LEADS - RESOLVE O AVISO 'leads is declared but never read' */}
        {activeTab === 'leads' && (
          <section>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '40px' }}>
              <h2>Leads Ativos ({session?.companyName})</h2>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={fetchLeads}>
                {loading ? 'Sincronizando...' : 'Atualizar Dados'}
              </button>
            </div>
            
            <div className="grid-container">
              {leads.length > 0 ? leads.map((lead, idx) => (
                <div className="card" key={idx}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                    <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Etapa 0{lead.fase}</span>
                    <span style={{ fontSize: '12px', color: lead.status === 'bot' ? '#25d366' : '#3b82f6' }}>
                      {lead.status === 'bot' ? '● Bot Ativo' : '● Humano'}
                    </span>
                  </div>
                  <div style={{ fontSize: '18px', fontWeight: 600, marginBottom: '4px' }}>{lead.nome || 'Lead'}</div>
                  <div style={{ fontSize: '13px', color: 'var(--text-muted)', marginBottom: '16px' }}>{lead.telefone}</div>
                  
                  <div style={{ display: 'flex', gap: '10px' }}>
                    <button className="btn-primary" style={{ padding: '8px 12px', fontSize: '13px' }}>Ver Chat</button>
                    <button style={{ flex: 1, background: 'transparent', border: '1px solid var(--border-line)', color: '#fff', borderRadius: '8px', cursor: 'pointer' }}>Assumir</button>
                  </div>
                </div>
              )) : (
                <p style={{ color: 'var(--text-muted)' }}>Nenhum lead encontrado para esta empresa.</p>
              )}
            </div>
          </section>
        )}

        {/* ABA: FLUXO DE 9 ETAPAS */}
        {activeTab === 'flow' && (
          <section>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '40px' }}>
              <div>
                <h2>Configuração do Robô</h2>
                <p style={{ color: 'var(--text-muted)', margin: 0 }}>Defina as 9 perguntas que o bot fará aos seus clientes.</p>
              </div>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={handleDeployFlow}>
                {loading ? 'Salvando...' : 'Salvar Alterações'}
              </button>
            </div>

            <div style={{ maxWidth: '800px' }}>
              {[1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => (
                <div key={n} className="step-box">
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 600, fontSize: '14px' }}>
                    <span>Pergunta {n}</span>
                    <span style={{ color: 'var(--text-muted)' }}>Coluna {String.fromCharCode(64 + n)} da Planilha</span>
                  </div>
                  <textarea rows={2} placeholder={`Digite a mensagem da etapa ${n}...`} defaultValue={n === 1 ? "Olá! Qual o seu nome?" : `Mensagem automática da etapa ${n}...`} />
                </div>
              ))}
            </div>
          </section>
        )}

        {/* ABA: INFRAESTRUTURA */}
        {activeTab === 'infra' && session?.role === 'admin' && (
          <section>
            <h2>Infraestrutura Geral (Super Admin)</h2>
            <p style={{ color: 'var(--text-muted)' }}>Visão geral de todas as empresas registradas no seu SaaS.</p>
            <div className="card" style={{ marginTop: '20px', maxWidth: '400px' }}>
              <div style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: '10px' }}>Empresas Ativas: 2</div>
              <div style={{ padding: '10px', background: 'var(--bg-deep)', borderRadius: '8px', marginBottom: '10px' }}>
                <strong>Solution Admin</strong> <br/><span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>ID: MASTER</span>
              </div>
              <div style={{ padding: '10px', background: 'var(--bg-deep)', borderRadius: '8px' }}>
                <strong>Empresa Cliente</strong> <br/><span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>ID: CLIENT_01</span>
              </div>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}

export default App;