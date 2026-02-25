import { useState, useEffect } from 'react';
import './App.css';

interface Lead {
  id: string;
  nome?: string;
  telefone: string;
  status: 'bot' | 'negociacao' | 'concluida' | 'perdida';
  fase: number;
}

interface UserSession {
  companyId: string;
  companyName: string;
  role: 'admin' | 'client';
}

function App() {
  const [currentView, setCurrentView] = useState<'login' | 'register' | 'dashboard'>('login');
  const [session, setSession] = useState<UserSession | null>(null);
  const [activeTab, setActiveTab] = useState('leads');
  
  const [leads, setLeads] = useState<Lead[]>([]);
  const [adminCompanies, setAdminCompanies] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const API_URL = "https:/contact-solution-whatsapp-1.onrender.com/";

  // ==========================================
  // FUNÇÕES DE COMUNICAÇÃO COM O BACKEND
  // ==========================================

  const handleLogin = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const email = formData.get('email') as string;
    const password = formData.get('password') as string;

    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      const data = await response.json();
      if(response.ok) {
        setSession({ companyId: data.companyId, companyName: data.companyName, role: data.role as 'admin' | 'client' });
        setCurrentView('dashboard');
      } else { alert(data.error || "Credenciais inválidas"); }
    } catch (error) { alert("Erro de conexão."); } 
    finally { setLoading(false); }
  };

  const handleRegister = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const data = Object.fromEntries(formData.entries());

    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });
      const result = await response.json();
      if(response.ok) {
        alert("Empresa registrada com sucesso! Faça login.");
        setCurrentView('login');
      } else { alert("Erro: " + result.error); }
    } catch (error) { alert("Erro de conexão."); } 
    finally { setLoading(false); }
  };

  const fetchLeads = async () => {
    if (!session) return;
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/leads/${session.companyId}`);
      if (response.ok) {
        const data = await response.json();
        const formatados = data.map((d: any) => ({
          ...d,
          status: d.status === 'open' ? 'bot' : (d.status_funil || 'negociacao')
        }));
        setLeads(formatados);
      }
    } catch (error) { console.error(error); } 
    finally { setLoading(false); }
  };

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
      if (response.ok) alert("Fluxo salvo e atualizado!");
    } catch (error) { alert("Erro ao salvar."); } 
    finally { setLoading(false); }
  };

  const handleChangePassword = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const novaSenha = formData.get('novaSenha') as string;
    
    if (!session) return;
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/auth/change-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ companyId: session.companyId, novaSenha })
      });
      if (response.ok) {
        alert("Senha alterada com sucesso!");
        e.currentTarget.reset();
      } else { alert("Erro ao alterar senha."); }
    } catch (error) { alert("Erro de conexão."); } 
    finally { setLoading(false); }
  };

  const fetchAdminCompanies = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/admin/companies`);
      if (response.ok) {
        const data = await response.json();
        setAdminCompanies(data.companies || []);
      }
    } catch (error) { console.error(error); } 
    finally { setLoading(false); }
  };

  const handleDeleteCompany = async (targetCompanyId: string) => {
    if (!window.confirm("Certeza absoluta que deseja apagar esta empresa? Todos os dados dela serão perdidos.")) return;
    
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/admin/companies/${targetCompanyId}`, {
        method: 'DELETE'
      });
      if (response.ok) {
        alert("Empresa deletada com sucesso.");
        fetchAdminCompanies(); // Atualiza a lista
      } else { alert("Erro ao deletar."); }
    } catch (error) { alert("Erro de conexão."); } 
    finally { setLoading(false); }
  };

  useEffect(() => {
    if (currentView === 'dashboard') {
      if (activeTab === 'leads') fetchLeads();
      if (activeTab === 'infra' && session?.role === 'admin') fetchAdminCompanies();
    }
  }, [currentView, activeTab]);

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
            <button type="submit" className="btn-primary" disabled={loading}> {loading ? 'Aguarde...' : 'Entrar no Sistema'} </button>
          </form>
          <button className="btn-link" onClick={() => setCurrentView('register')}>Nova empresa? Registre-se aqui</button>
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
            <button type="submit" className="btn-primary" disabled={loading}> {loading ? 'Criando...' : 'Registrar Empresa'} </button>
          </form>
          <button className="btn-link" onClick={() => setCurrentView('login')}>Já tem uma conta? Faça login</button>
        </div>
      </div>
    );
  }

  // --- FILTROS DO FUNIL DE VENDAS ---
  const leadsBot = leads.filter(l => l.status === 'bot');
  const leadsNegociacao = leads.filter(l => l.status === 'negociacao');
  const leadsConcluida = leads.filter(l => l.status === 'concluida');
  const leadsPerdida = leads.filter(l => l.status === 'perdida');

  return (
    <div className="app-layout">
      {/* MENU LATERAL */}
      <aside className="sidebar">
        <div style={{ fontSize: '20px', fontWeight: 'bold', color: '#fff', marginBottom: '10px' }}>ContactSolution</div>
        <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '40px' }}>
          Logado como: <br/><strong style={{color: '#fff'}}>{session?.companyName}</strong>
        </div>

        <nav style={{ flex: 1 }}>
          <div className={`nav-item ${activeTab === 'leads' ? 'active' : ''}`} onClick={() => setActiveTab('leads')}>📊 Funil de Vendas</div>
          <div className={`nav-item ${activeTab === 'flow' ? 'active' : ''}`} onClick={() => setActiveTab('flow')}>⚙️ Configurar Fluxo</div>
          <div className={`nav-item ${activeTab === 'profile' ? 'active' : ''}`} onClick={() => setActiveTab('profile')}>👤 Meu Perfil</div>
          
          {session?.role === 'admin' && (
            <div className={`nav-item ${activeTab === 'infra' ? 'active' : ''}`} onClick={() => setActiveTab('infra')} style={{marginTop: '20px', borderTop: '1px solid var(--border-line)', paddingTop: '15px'}}>
              🏢 Controle Master
            </div>
          )}
        </nav>
        <button className="btn-link" style={{ textAlign: 'left', color: '#ef4444' }} onClick={() => {setSession(null); setCurrentView('login');}}>Sair do Sistema</button>
      </aside>

      {/* CONTEÚDO PRINCIPAL */}
      <main className="main-content" style={{ padding: '20px 40px' }}>
        
        {/* ABA: LEADS (FUNIL KANBAN) */}
        {activeTab === 'leads' && (
          <section style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
              <h2>Funil de Vendas</h2>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={fetchLeads}>Atualizar Funil</button>
            </div>
            
            <div className="kanban-board">
              <div className="kanban-column">
                <div className="kanban-header" style={{ borderTop: '3px solid #3b82f6' }}>🤖 Robô Atendendo <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsBot.length}</span></div>
                <div className="kanban-cards">
                  {leadsBot.map((lead, idx) => (
                    <div className="card" key={idx}>
                      <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '4px' }}>Etapa {lead.fase || 1}/9</div>
                      <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead s/ nome'}</div>
                      <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="kanban-column">
                <div className="kanban-header" style={{ borderTop: '3px solid #eab308' }}>🗣️ Em Negociação <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsNegociacao.length}</span></div>
                <div className="kanban-cards">
                  {leadsNegociacao.map((lead, idx) => (
                    <div className="card" key={idx}>
                      <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead'}</div>
                      <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="kanban-column">
                <div className="kanban-header" style={{ borderTop: '3px solid #22c55e' }}>✅ Venda Concluída <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsConcluida.length}</span></div>
                <div className="kanban-cards">
                  {leadsConcluida.map((lead, idx) => (
                    <div className="card" key={idx}>
                      <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead'}</div>
                      <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="kanban-column">
                <div className="kanban-header" style={{ borderTop: '3px solid #ef4444' }}>❌ Não Concluída <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsPerdida.length}</span></div>
                <div className="kanban-cards">
                  {leadsPerdida.map((lead, idx) => (
                    <div className="card" key={idx}>
                      <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead'}</div>
                      <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>
        )}

        {/* ABA: FLUXO */}
        {activeTab === 'flow' && (
          <section>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '40px' }}>
              <div><h2>Configuração do Robô</h2><p style={{ color: 'var(--text-muted)', margin: 0 }}>Defina as 9 perguntas que o bot fará.</p></div>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={handleDeployFlow}>Salvar Alterações</button>
            </div>
            <div style={{ maxWidth: '800px' }}>
              {[1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => (
                <div key={n} className="step-box">
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 600, fontSize: '14px' }}>
                    <span>Pergunta {n}</span>
                    <span style={{ color: 'var(--text-muted)' }}>Coluna {String.fromCharCode(64 + n)} da Planilha</span>
                  </div>
                  <textarea rows={2} placeholder={`Mensagem da etapa ${n}...`} defaultValue={n === 1 ? "Olá! Qual o seu nome?" : `Mensagem automática da etapa ${n}...`} />
                </div>
              ))}
            </div>
          </section>
        )}

        {/* ABA: PERFIL (TROCAR SENHA) */}
        {activeTab === 'profile' && (
          <section>
            <h2>Meu Perfil</h2>
            <p style={{ color: 'var(--text-muted)', marginBottom: '30px' }}>Gerencie as configurações de segurança da sua conta.</p>
            <div className="card" style={{ maxWidth: '400px' }}>
              <h3 style={{ marginTop: 0 }}>Alterar Senha</h3>
              <form onSubmit={handleChangePassword}>
                <input type="password" name="novaSenha" placeholder="Digite a nova senha" className="input-field" required minLength={3} />
                <button type="submit" className="btn-primary" disabled={loading}>{loading ? 'Salvando...' : 'Atualizar Senha'}</button>
              </form>
            </div>
          </section>
        )}

        {/* ABA: INFRAESTRUTURA (SÓ ADMIN VÊ) */}
        {activeTab === 'infra' && session?.role === 'admin' && (
          <section>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '40px' }}>
              <div><h2>Painel de Controle (Master)</h2><p style={{ color: 'var(--text-muted)', margin: 0 }}>Gerencie todos os clientes do seu SaaS.</p></div>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={fetchAdminCompanies}>Atualizar Lista</button>
            </div>

            <div className="grid-container">
              {adminCompanies.map((empresa, idx) => (
                <div className="card" key={idx} style={{ borderLeft: '4px solid var(--btn-blue)' }}>
                  <div style={{ fontSize: '18px', fontWeight: 600, marginBottom: '4px' }}>{empresa.name}</div>
                  <div style={{ fontSize: '13px', color: 'var(--text-muted)', marginBottom: '4px' }}>E-mail: {empresa.email}</div>
                  <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '20px' }}>ID: {empresa.id}</div>
                  <button onClick={() => handleDeleteCompany(empresa.id)} style={{ width: '100%', background: '#ef4444', color: '#fff', border: 'none', padding: '10px', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}>
                    Apagar Empresa
                  </button>
                </div>
              ))}
              {adminCompanies.length === 0 && <p style={{ color: 'var(--text-muted)' }}>Nenhuma empresa encontrada no banco.</p>}
            </div>
          </section>
        )}
      </main>
    </div>
  );
}

export default App;