import { useState, useEffect } from 'react';
import { DragDropContext, Droppable, Draggable, type DropResult } from '@hello-pangea/dnd';
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
  const [currentView, setCurrentView] = useState<'login' | 'dashboard'>('login');
  const [session, setSession] = useState<UserSession | null>(null);
  const [activeTab, setActiveTab] = useState('leads');
  
  const [leads, setLeads] = useState<Lead[]>([]);
  const [adminCompanies, setAdminCompanies] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  
  const [flowMessages, setFlowMessages] = useState<string[]>(Array(9).fill(''));

  const API_URL = "https://contact-solution-whatsapp-1.onrender.com";

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
        alert("Empresa registrada com sucesso no sistema!");
        e.currentTarget.reset();
        fetchAdminCompanies();
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

  const fetchFlowMessages = async () => {
    if (!session) return;
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/config/flow/${session.companyId}`);
      if (response.ok) {
        const data = await response.json();
        if (data.messages && data.messages.length > 0) {
          setFlowMessages(data.messages);
        }
      }
    } catch (error) { console.error(error); } 
    finally { setLoading(false); }
  };

  const handleDeployFlow = async () => {
    if (!session) return;
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/config/flow`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ companyId: session.companyId, flow_messages: flowMessages })
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
        fetchAdminCompanies(); 
      } else { alert("Erro ao deletar."); }
    } catch (error) { alert("Erro de conexão."); } 
    finally { setLoading(false); }
  };

  // ==========================================
  // NOVO: LÓGICA DE ARRASTAR E SOLTAR (DRAG AND DROP)
  // ==========================================
  const onDragEnd = async (result: DropResult) => {
    const { source, destination, draggableId } = result;

    // Se o usuário soltar fora do Kanban, não faz nada
    if (!destination) return;

    // Se soltar exatamente onde já estava, não faz nada
    if (source.droppableId === destination.droppableId && source.index === destination.index) return;

    // Pega o ID da coluna de destino (novo status)
    const newStatus = destination.droppableId as Lead['status'];

    // 1. Atualiza o visual da tela imediatamente (Optimistic UI) para não dar delay pro cliente
    setLeads((prevLeads) => 
      prevLeads.map((lead) => 
        String(lead.id) === draggableId ? { ...lead, status: newStatus } : lead
      )
    );

    // 2. Manda a alteração silenciosamente pro Backend (Render)
    try {
      await fetch(`${API_URL}/api/leads/${draggableId}/status`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus })
      });
    } catch (error) {
      console.error("Erro ao salvar o novo status no banco:", error);
    }
  };

  useEffect(() => {
    if (currentView === 'dashboard') {
      if (activeTab === 'leads') fetchLeads();
      if (activeTab === 'infra' && session?.role === 'admin') fetchAdminCompanies();
      if (activeTab === 'flow') fetchFlowMessages();
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
          <div className="auth-subtitle">Acesso Restrito</div>
          <form onSubmit={handleLogin}>
            <input type="email" name="email" placeholder="E-mail corporativo" className="input-field" required />
            <input type="password" name="password" placeholder="Senha" className="input-field" required />
            <button type="submit" className="btn-primary" disabled={loading}> {loading ? 'Aguarde...' : 'Entrar no Sistema'} </button>
          </form>
        </div>
      </div>
    );
  }

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
        
        {/* ABA: LEADS (FUNIL KANBAN COM DRAG AND DROP) */}
        {activeTab === 'leads' && (
          <section style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
              <h2>Funil de Vendas</h2>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={fetchLeads}>Atualizar Funil</button>
            </div>
            
            {/* O DragDropContext abraça todas as colunas */}
            <DragDropContext onDragEnd={onDragEnd}>
              <div className="kanban-board">
                
                {/* COLUNA: ROBÔ */}
                <Droppable droppableId="bot">
                  {(provided) => (
                    <div className="kanban-column" ref={provided.innerRef} {...provided.droppableProps}>
                      <div className="kanban-header" style={{ borderTop: '3px solid #3b82f6' }}>🤖 Robô Atendendo <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsBot.length}</span></div>
                      <div className="kanban-cards">
                        {leadsBot.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div className="card" ref={provided.innerRef} {...provided.draggableProps} {...provided.dragHandleProps} style={{...provided.draggableProps.style}}>
                                <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '4px' }}>Etapa {lead.fase || 1}/9</div>
                                <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead s/ nome'}</div>
                                <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                              </div>
                            )}
                          </Draggable>
                        ))}
                        {provided.placeholder}
                      </div>
                    </div>
                  )}
                </Droppable>

                {/* COLUNA: NEGOCIAÇÃO */}
                <Droppable droppableId="negociacao">
                  {(provided) => (
                    <div className="kanban-column" ref={provided.innerRef} {...provided.droppableProps}>
                      <div className="kanban-header" style={{ borderTop: '3px solid #eab308' }}>🗣️ Em Negociação <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsNegociacao.length}</span></div>
                      <div className="kanban-cards">
                        {leadsNegociacao.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div className="card" ref={provided.innerRef} {...provided.draggableProps} {...provided.dragHandleProps} style={{...provided.draggableProps.style}}>
                                <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead'}</div>
                                <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                              </div>
                            )}
                          </Draggable>
                        ))}
                        {provided.placeholder}
                      </div>
                    </div>
                  )}
                </Droppable>

                {/* COLUNA: CONCLUÍDA */}
                <Droppable droppableId="concluida">
                  {(provided) => (
                    <div className="kanban-column" ref={provided.innerRef} {...provided.droppableProps}>
                      <div className="kanban-header" style={{ borderTop: '3px solid #22c55e' }}>✅ Venda Concluída <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsConcluida.length}</span></div>
                      <div className="kanban-cards">
                        {leadsConcluida.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div className="card" ref={provided.innerRef} {...provided.draggableProps} {...provided.dragHandleProps} style={{...provided.draggableProps.style}}>
                                <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead'}</div>
                                <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                              </div>
                            )}
                          </Draggable>
                        ))}
                        {provided.placeholder}
                      </div>
                    </div>
                  )}
                </Droppable>

                {/* COLUNA: PERDIDA */}
                <Droppable droppableId="perdida">
                  {(provided) => (
                    <div className="kanban-column" ref={provided.innerRef} {...provided.droppableProps}>
                      <div className="kanban-header" style={{ borderTop: '3px solid #ef4444' }}>❌ Não Concluída <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsPerdida.length}</span></div>
                      <div className="kanban-cards">
                        {leadsPerdida.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div className="card" ref={provided.innerRef} {...provided.draggableProps} {...provided.dragHandleProps} style={{...provided.draggableProps.style}}>
                                <div style={{ fontWeight: 600 }}>{lead.nome || 'Lead'}</div>
                                <div style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{lead.telefone}</div>
                              </div>
                            )}
                          </Draggable>
                        ))}
                        {provided.placeholder}
                      </div>
                    </div>
                  )}
                </Droppable>

              </div>
            </DragDropContext>
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
              {[0, 1, 2, 3, 4, 5, 6, 7, 8].map((idx) => {
                const n = idx + 1;
                return (
                  <div key={n} className="step-box">
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 600, fontSize: '14px' }}>
                      <span>Pergunta {n}</span>
                      <span style={{ color: 'var(--text-muted)' }}>Coluna {String.fromCharCode(64 + n)} da Planilha</span>
                    </div>
                    <textarea 
                      rows={2} 
                      placeholder={`Mensagem da etapa ${n}...`} 
                      value={flowMessages[idx] || ''} 
                      onChange={(e) => {
                        const novasMensagens = [...flowMessages];
                        novasMensagens[idx] = e.target.value;
                        setFlowMessages(novasMensagens);
                      }}
                    />
                  </div>
                );
              })}
            </div>
          </section>
        )}

        {/* ABA: PERFIL */}
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

        {/* ABA: INFRAESTRUTURA */}
        {activeTab === 'infra' && session?.role === 'admin' && (
          <section>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
              <div><h2>Painel de Controle (Master)</h2><p style={{ color: 'var(--text-muted)', margin: 0 }}>Gerencie todos os clientes do seu SaaS.</p></div>
              <button className="btn-primary" style={{ width: 'auto' }} onClick={fetchAdminCompanies}>Atualizar Lista</button>
            </div>

            <div className="card" style={{ marginBottom: '40px', borderLeft: '4px solid #22c55e' }}>
              <h3 style={{ marginTop: 0, marginBottom: '20px' }}>➕ Cadastrar Novo Cliente</h3>
              <form onSubmit={handleRegister} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px' }}>
                <input type="text" name="companyName" placeholder="Nome da Empresa" className="input-field" required />
                <input type="email" name="email" placeholder="E-mail administrador" className="input-field" required />
                <input type="tel" name="whatsapp" placeholder="WhatsApp do Bot" className="input-field" required />
                <input type="password" name="password" placeholder="Crie uma senha provisória" className="input-field" required />
                <div style={{ gridColumn: '1 / -1' }}>
                  <button type="submit" className="btn-primary" disabled={loading} style={{ width: '250px' }}> 
                    {loading ? 'Criando...' : 'Registrar Nova Empresa'} 
                  </button>
                </div>
              </form>
            </div>

            <h3 style={{ marginBottom: '20px' }}>🏢 Empresas Ativas no Sistema</h3>
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