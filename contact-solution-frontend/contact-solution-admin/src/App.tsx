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
  email: string;
}

function App() {
  const [selectedContact, setSelectedContact] = useState<any>(null);
  useEffect(() => {
    setSelectedContact({ name: "Meu Teste", phone: "5511964816315" });
  }, []);
  const [selectedLead, setSelectedLead] = useState<Lead | null>(null);
  const [chatMessages, setChatMessages] = useState<any[]>([]);
  const [currentView, setCurrentView] = useState<'login' | 'dashboard'>(() => {
  return localStorage.getItem('userSession') ? 'dashboard' : 'login';
});
  const [session, setSession] = useState<UserSession | null>(() => {
  const savedSession = localStorage.getItem('userSession');
  return savedSession ? JSON.parse(savedSession) : null;
});
  const [activeTab, setActiveTab] = useState('leads');

  const [leads, setLeads] = useState<Lead[]>([]);
  const [adminCompanies, setAdminCompanies] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const [flowMessages, setFlowMessages] = useState<string[]>(Array(9).fill(''));

  const API_URL = "https://contact-solution-whatsapp-1.onrender.com";
  
  const [showForgot, setShowForgot] = useState(false);
  const [forgotEmail, setForgotEmail] = useState("");

 const [newMessage, setNewMessage] = useState('');
const handleSendMessage = async () => {
  if (!newMessage.trim() || !selectedContact) return;

  try {
    // 🎯 MUDAMOS A URL PARA A ROTA ANTIGA
    const response = await fetch('http://localhost:8000/api/send-message', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        companyId: "MASTER", // Batendo com o que o Python espera
        phone: selectedContact.phone,
        text: newMessage
      }),
    });

    if (response.ok) {
      setNewMessage('');
      // fetchMessages(); // Atualiza a tela
    }
  } catch (error) {
    console.error("Erro ao conectar com a API antiga:", error);
  }
};

const handleRecoverPassword = async () => {
    alert("Função de recuperação de senha em manutenção.");
};

  // ==========================================
  // FUNÇÕES DE COMUNICAÇÃO COM O BACKEND
  // ==========================================

  // 1. LOGIN
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

      if (response.ok) {
        // --- ESTA LINHA SALVA O LOGIN NO NAVEGADOR PARA O F5 NÃO TE DERRUBAR ---
        localStorage.setItem('userSession', JSON.stringify(data)); 
        
        setSession({ 
          companyId: data.companyId, 
          companyName: data.companyName, 
          role: data.role as 'admin' | 'client',
          email: data.email 
        });
        setCurrentView('dashboard');
      } else {
        // Caso as credenciais estejam erradas
        alert(data.message || data.error || "Credenciais inválidas");
      }
    } catch (error) {
      // Caso o servidor (Render) esteja fora do ar ou erro de rede
      console.error("Erro no login:", error);
      alert("Erro de conexão com o servidor.");
    } finally {
      // Para o spinner de carregamento aconteça o que acontecer
      setLoading(false);
    }
  };

// 2. REGISTRAR EMPRESA (Livre de fantasmas!)
  const handleRegister = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    
    // 🔥 O SEGREDO: Salvar o formulário na memória antes de qualquer "await"
    const form = e.currentTarget; 
    
    setLoading(true);
    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());

    try {
      const response = await fetch(`${API_URL}/api/admin/companies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });
      
      const result = await response.json();
      
      if (response.ok) {
        alert("Empresa registrada com sucesso no sistema!");
        form.reset(); // Agora ele vai limpar perfeitamente
        fetchAdminCompanies(); 
      } else { 
        alert("Erro: " + result.error); 
      }
    } catch (error) { 
      // Mudamos o texto para ficar mais claro caso ocorra um erro real
      console.error(error); 
      alert("Erro ao processar o formulário."); 
    } finally { 
      setLoading(false); 
    }
  };

  // 3. APAGAR EMPRESA
  const handleDeleteCompany = async (id: string) => {
    if (!window.confirm("⚠️ ATENÇÃO: Isso apagará a empresa e todos os dados dela. Continuar?")) return;

    try {
      const response = await fetch(`${API_URL}/api/admin/companies/${id}`, {
        method: 'DELETE',
      });
      if (response.ok) {
        alert("Empresa removida com sucesso!");
        fetchAdminCompanies();
      } else {
        alert("Erro ao remover empresa.");
      }
    } catch (error) {
      alert("Erro de conexão com o servidor.");
    }
  };

  // 4. EDITAR EMPRESA
  const handleEditCompany = async (empresa: any) => {
    const novoNome = prompt("Novo nome da empresa:", empresa.name);
    const novoBot = prompt("Novo WhatsApp do Bot:", empresa.bot_whatsapp);

    if (novoNome && novoBot) {
      try {
        const response = await fetch(`${API_URL}/api/admin/companies/${empresa.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: novoNome,
            email: empresa.email,
            phone: empresa.phone,
            bot_whatsapp: novoBot,
            password: "" 
          })
        });

        if (response.ok) {
          alert("Dados atualizados!");
          fetchAdminCompanies();
        }
      } catch (error) {
        alert("Erro ao atualizar.");
      }
    }
  };

  // 5. ENVIAR MENSAGEM
  const sendMessage = async () => {
    if (!newMessage.trim() || !selectedLead) return;

    const tempMsg = { direction: 'outbound', text: newMessage, created_at: new Date().toISOString() };
    setChatMessages(prev => [...prev, tempMsg]);
    
    const textToSend = newMessage;
    setNewMessage(""); 

    try {
      const response = await fetch(`${API_URL}/api/messages/send`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          company_id: (selectedLead as any).company_id,
          phone: selectedLead.telefone,
          text: textToSend
        })
      });

      if (!response.ok) {
        console.error("Falha ao salvar a mensagem no banco");
      }
    } catch (error) {
      console.error("Erro de rede:", error);
    }
  };

  // 6. BUSCAR LEADS
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

  // 7. BUSCAR MENSAGENS DO FLUXO
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

  // 8. SALVAR FLUXO
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

// 9. TROCAR SENHA (VERSÃO BLINDADA)
  const handleChangePassword = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    
    // Tenta pegar o email da sessão ou direto do "crachá" no navegador
    const savedSession = JSON.parse(localStorage.getItem('userSession') || '{}');
    const userEmail = session?.email || savedSession.email;

    console.log("🚀 Iniciando troca de senha para:", userEmail);

    const formData = new FormData(e.currentTarget);
    const password = formData.get('novaSenha') as string;

    if (!userEmail) {
      console.error("❌ PAREDE INVISÍVEL: E-mail não encontrado em lugar nenhum!");
      alert("Erro: O sistema não identificou seu e-mail. Saia e entre novamente.");
      return;
    }

    setLoading(true);
    try {
      // ATENÇÃO: Verifique se no seu app.py a rota é exatamente 'change-password' ou 'update-password'
      const response = await fetch(`${API_URL}/api/auth/change-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          password: password, 
          email: userEmail 
        }),
      });

      const resData = await response.json();

      if (response.ok) {
        alert("Senha alterada com sucesso! ✅");
        (e.target as HTMLFormElement).reset();
      } else {
        alert("Erro do servidor: " + (resData.message || resData.error || "Erro desconhecido"));
      }
    } catch (error) {
      alert("Erro de conexão. O servidor está ligado?");
    } finally {
      setLoading(false);
    }
  };

  // 10. BUSCAR EMPRESAS NO ADMIN
const fetchAdminCompanies = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/admin/companies`);
      if (response.ok) {
        const data = await response.json();
        
        // 🔥 A MÁGICA ESTÁ AQUI NA LINHA DE BAIXO:
        const listaDeEmpresas = Array.isArray(data) ? data : (data.companies || []);
        
        setAdminCompanies(listaDeEmpresas);
      }
    } catch (error) { 
      console.error(error); 
    } finally { 
      setLoading(false); 
    }
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

  const openChat = async (lead: Lead) => {
    setSelectedLead(lead);
    try {
      const response = await fetch(`${API_URL}/api/messages/${session?.companyId}/${lead.telefone}`);
      if (response.ok) {
        const data = await response.json();
        setChatMessages(data);
      }
    } catch (error) {
      console.error("Erro ao carregar chat", error);
    }
  };

useEffect(() => {
  let interval: any;
  if (selectedLead) {
    interval = setInterval(() => {
      fetch(`${API_URL}/api/messages/${session?.companyId}/${selectedLead.telefone}`)
        .then(res => res.json())
        .then(data => setChatMessages(data))
        .catch(err => console.error("Erro na atualização rápida:", err));
    }, 2000); 
  }
  return () => clearInterval(interval);
}, [selectedLead, session?.companyId]);


  useEffect(() => {
    if (currentView === 'dashboard') {
      if (activeTab === 'leads') fetchLeads();
      if (activeTab === 'infra' && session?.role === 'admin') fetchAdminCompanies();
      if (activeTab === 'flow') fetchFlowMessages();
    }
  }, [currentView, activeTab]);

  // ==========================================
  // CONFIGURAÇÕES VISUAIS DO CRM (BRANDING)
  // ==========================================
  const crmCardStyle = {
    backgroundColor: '#1E293B',
    padding: '20px',
    borderRadius: '16px',
    border: '1px solid #334155',
    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.2)',
    display: 'flex',
    flexDirection: 'column' as const,
    gap: '4px'
  };

  const labelStyle = { color: '#94A3B8', fontSize: '13px', fontWeight: '500' };
  const valueStyle = { color: '#FFFFFF', fontSize: '28px', fontWeight: '800' };
  const subLabelStyle = { fontSize: '10px', fontWeight: 'bold' as const, textTransform: 'uppercase' as const, marginTop: '4px' };

  const statusBadgeStyle = (color: string) => ({
    backgroundColor: color,
    padding: '8px 16px',
    borderRadius: '20px',
    color: '#FFFFFF',
    fontSize: '12px',
    fontWeight: 'bold' as const,
    whiteSpace: 'nowrap' as const,
    border: '1px solid rgba(255,255,255,0.1)',
    display: 'flex',          // Fundamental para alinhar
    alignItems: 'center',      // Centraliza verticalmente
    justifyContent: 'center',   // Centraliza horizontalmente
    gap: '8px',                // Espaço entre o emoji e o texto
    lineHeight: '1',           // Evita que o texto "suba" ou "desça"
    minWidth: '130px'          // Garante que todas tenham um tamanho harmonioso
  });
// ==========================================
  // RENDERIZAÇÃO DAS TELAS (PREMIUM DARK MODE)
  // ==========================================
// ==========================================
  // NOVA TELA HOME + LOGIN (SPLIT SCREEN)
  // ==========================================
  if (currentView === 'login') {
    // Definimos os estilos aqui dentro para facilitar a colagem
    const inputStyle = {
      width: '100%',
      padding: '14px',
      backgroundColor: '#0F172A',
      border: '1px solid #334155',
      borderRadius: '12px',
      color: '#FFFFFF',
      outline: 'none',
      boxSizing: 'border-box' as 'border-box'
    };

    const buttonStyle = {
      width: '100%',
      padding: '16px',
      backgroundColor: '#5A7FFF', // Technical Blue da sua marca
      color: '#FFFFFF',
      border: 'none',
      borderRadius: '12px',
      fontSize: '16px',
      fontWeight: 'bold' as 'bold',
      cursor: 'pointer',
      boxShadow: '0 10px 15px -3px rgba(90, 127, 255, 0.3)',
      marginTop: '10px',
      transition: 'all 0.2s ease'
    };

    return (
      <div style={{
        minHeight: '100vh',
        display: 'flex',
        backgroundColor: '#1A1A2E', // Primary Navy da sua marca
        fontFamily: "'Inter', sans-serif",
        color: '#FFFFFF'
      }}>
        
        {/* LADO ESQUERDO: LOGIN */}
        <div style={{ flex: '1', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '40px', zIndex: 2 }}>
          <div style={{ width: '100%', maxWidth: '400px', backgroundColor: 'rgba(30, 41, 59, 0.5)', padding: '40px', borderRadius: '24px', backdropFilter: 'blur(10px)', border: '1px solid rgba(255, 255, 255, 0.1)', boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5)' }}>
            
            <div style={{ marginBottom: '32px', textAlign: 'center' }}>
              <h2 style={{ fontSize: '24px', fontWeight: 'bold', margin: '0 0 8px 0' }}>Acesso ao Sistema</h2>
              <p style={{ color: '#94A3B8', fontSize: '14px' }}>Bem-vindo à Contact Solution</p>
            </div>

            <form onSubmit={handleLogin} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              <div style={{ textAlign: 'left' }}>
                <label style={{ display: 'block', color: '#94A3B8', fontSize: '12px', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>E-mail corporativo</label>
                <input type="email" name="email" required placeholder="seu@email.com" style={inputStyle} />
              </div>

              <div style={{ textAlign: 'left' }}>
                <label style={{ display: 'block', color: '#94A3B8', fontSize: '12px', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>Senha</label>
                <input type="password" name="password" required placeholder="••••••••" style={inputStyle} />
                
                {/* LINK DE RECUPERAÇÃO */}
                <div style={{ textAlign: 'right', marginTop: '8px' }}>
                  <button
                    type="button"
                    onClick={() => setShowForgot(true)}
                    style={{ background: 'none', border: 'none', color: '#94A3B8', fontSize: '12px', cursor: 'pointer', padding: 0 }}
                  >
                    Esqueci minha senha
                  </button>
                </div>
              </div>

              <button type="submit" disabled={loading} style={buttonStyle}>
                {loading ? 'Autenticando...' : 'Entrar na Plataforma'}
              </button>
            </form>

            <p style={{ marginTop: '30px', fontSize: '11px', color: '#64748B', textAlign: 'center' }}>
              © 2026 Contact Solution SaaS. <br/>Tecnologia de ponta para conversões reais.
            </p>
          </div>
        </div>

{/* 🛡️ MODAL DE RECUPERAÇÃO DE SENHA (DARK MODE PREMIUM) */}
      {showForgot && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          width: '100%',
          height: '100%',
          backgroundColor: 'rgba(0, 0, 0, 0.85)',
          backdropFilter: 'blur(10px)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 9999,
          padding: '20px'
        }}>
          <div style={{
            backgroundColor: '#1E293B',
            border: '1px solid rgba(255, 255, 255, 0.1)',
            padding: '40px',
            borderRadius: '24px',
            width: '100%',
            maxWidth: '400px',
            textAlign: 'center',
            boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5)',
            animation: 'fadeIn 0.3s ease-out'
          }}>
            <h2 style={{ color: 'white', fontSize: '24px', fontWeight: 'bold', marginBottom: '8px' }}>
              Recuperar Senha
            </h2>
            <p style={{ color: '#94A3B8', fontSize: '14px', marginBottom: '24px', lineHeight: '1.5' }}>
              Informe seu e-mail de cadastro. Enviaremos uma nova senha temporária para você acessar o sistema.
            </p>
            
            <div style={{ textAlign: 'left', marginBottom: '20px' }}>
              <label style={{ display: 'block', color: '#94A3B8', fontSize: '12px', marginBottom: '8px', textTransform: 'uppercase' }}>
                E-mail de Recuperação
              </label>
              <input
                type="email"
                placeholder="seu@email.com"
                value={forgotEmail}
                onChange={(e) => setForgotEmail(e.target.value)}
                style={{ 
                  ...inputStyle, 
                  width: '100%', 
                  boxSizing: 'border-box' 
                }}
              />
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <button 
                type="button"
                onClick={handleRecoverPassword}
                disabled={loading}
                style={{
                  ...buttonStyle,
                  width: '100%',
                  cursor: loading ? 'not-allowed' : 'pointer',
                  opacity: loading ? 0.7 : 1
                }}
              >
                {loading ? 'Processando...' : 'Gerar Nova Senha'}
              </button>
              
              <button
                type="button"
                onClick={() => setShowForgot(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  color: '#94A3B8',
                  cursor: 'pointer',
                  fontSize: '14px',
                  padding: '10px',
                  transition: 'color 0.2s'
                }}
                onMouseEnter={(e) => (e.currentTarget.style.color = 'white')}
                onMouseLeave={(e) => (e.currentTarget.style.color = '#94A3B8')}
              >
                Cancelar e Voltar
              </button>
            </div>
          </div>
        </div>
      )}

        {/* LADO DIREITO: BRANDING (A parte bonita!) */}
        <div style={{
          flex: '1.2',
          position: 'relative',
          display: 'none' as 'none', // Isso esconde no celular
          flexDirection: 'column' as 'column',
          alignItems: 'center',
          justifyContent: 'center',
          overflow: 'hidden',
          background: 'linear-gradient(135deg, #1A1A2E 0%, #002C3F 100%)',
          borderLeft: '1px solid rgba(255, 255, 255, 0.05)'
        }}>
          {/* Media Query Manual para Desktop */}
          <style>{`
            @media (min-width: 1024px) {
              div[style*="flex: 1.2"] { display: flex !important; }
            }
          `}</style>

          {/* EFEITO DE LUZ DE FUNDO */}
          <div style={{
            position: 'absolute',
            width: '600px',
            height: '600px',
            background: 'radial-gradient(circle, rgba(90, 127, 255, 0.15) 0%, transparent 70%)',
            zIndex: 1
          }}></div>

          <div style={{ zIndex: 2, textAlign: 'center', padding: '0 40px' }}>
            <img 
              src="/logocontactsolution.png" 
              alt="Logo" 
              style={{ width: '280px', marginBottom: '30px', filter: 'drop-shadow(0 0 20px rgba(90, 127, 255, 0.3))' }} 
            />
            <h1 style={{ fontSize: '48px', fontWeight: '800', marginBottom: '16px', letterSpacing: '-1px' }}>
              Potencialize suas <span style={{ color: '#5A7FFF' }}>Conexões</span>
            </h1>
            <p style={{ fontSize: '18px', color: '#94A3B8', maxWidth: '500px', lineHeight: '1.6' }}>
              A solução SaaS completa para automação de atendimento e gestão de leads em alta escala.
            </p>
          </div>
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
{/* MENU LATERAL (REESTILIZADO) */}
      <aside className="sidebar" style={{ backgroundColor: '#1A1A2E', borderRight: '1px solid #334155' }}>
        <div style={{ padding: '20px', textAlign: 'center', marginBottom: '10px' }}>
          {/* LOGO OFICIAL NO MENU */}
          <img 
            src="/logocontactsolution.png" 
            alt="Logo" 
            style={{ width: '100%', maxWidth: '140px', height: 'auto' }} 
          />
          <div style={{ 
            height: '1px', 
            background: 'linear-gradient(90deg, transparent, #5A7FFF, transparent)', 
            marginTop: '20px',
            opacity: 0.5 
          }}></div>
        </div>

        <div style={{ padding: '0 20px 30px', fontSize: '12px', color: '#94A3B8', textAlign: 'center' }}>
          Logado como: <br />
          <strong style={{ color: '#FFFFFF', fontSize: '13px' }}>{session?.companyName}</strong>
          <div style={{ 
            marginTop: '5px', 
            color: '#5A7FFF', 
            fontWeight: 'bold', 
            textTransform: 'uppercase',
            fontSize: '10px',
            letterSpacing: '1px'
          }}>
            {session?.role === 'admin' ? '🛡️ Master Admin' : '👤 Painel Cliente'}
          </div>
        </div>

        <nav style={{ flex: 1 }}>
          <div className={`nav-item ${activeTab === 'leads' ? 'active' : ''}`} onClick={() => setActiveTab('leads')}>📊 Funil de Vendas</div>
          <div className={`nav-item ${activeTab === 'flow' ? 'active' : ''}`} onClick={() => setActiveTab('flow')}>⚙️ Configurar Fluxo</div>
          <div className={`nav-item ${activeTab === 'profile' ? 'active' : ''}`} onClick={() => setActiveTab('profile')}>👤 Meu Perfil</div>

          {session?.role === 'admin' && (
            <div className={`nav-item ${activeTab === 'infra' ? 'active' : ''}`} onClick={() => setActiveTab('infra')} style={{ marginTop: '20px', borderTop: '1px solid var(--border-line)', paddingTop: '15px' }}>
              🏢 Controle Master
            </div>
          )}
        </nav>
        <button 
  className="btn-link" 
  style={{ textAlign: 'left', color: '#ef4444' }} 
  onClick={() => { 
    setSession(null); 
    localStorage.removeItem('userSession'); // <--- ESSA LINHA DESTRÓI O CRACHÁ
    setCurrentView('login'); 
  }}
>
  Sair do Sistema
</button>
      </aside>

      {/* CONTEÚDO PRINCIPAL */}
      <main className="main-content" style={{ padding: '20px 40px' }}>

        {/* ABA: LEADS (FUNIL KANBAN COM DRAG AND DROP) */}
        {activeTab === 'leads' && (
          <section style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>

       {/* 1. CABEÇALHO CENTRALIZADO E LIMPO */}
            <div style={{ textAlign: 'center', marginBottom: '40px', paddingTop: '20px' }}>
              <h1 style={{ 
                fontSize: '32px', 
                fontWeight: '800', 
                color: '#FFFFFF', 
                margin: '0 0 8px 0', 
                letterSpacing: '-1px' 
              }}>
                Gestão de Leads
              </h1>
              <p style={{ color: '#94A3B8', fontSize: '16px', margin: '0 0 20px 0' }}>
                Controle sua escala e conversão em tempo real.
              </p>

              {/* Botão de atualizar centralizado abaixo do texto */}
              <button 
                onClick={fetchLeads}
                style={{
                  backgroundColor: '#1E293B',
                  color: '#5A7FFF',
                  border: '1px solid #334155',
                  padding: '10px 24px',
                  borderRadius: '12px',
                  fontWeight: '600',
                  cursor: 'pointer',
                  transition: 'all 0.2s'
                }}
              >
                {loading ? 'Sincronizando...' : '🔄 Atualizar Funil'}
              </button>
            </div>

            {/* 2. CARDS DE MÉTRICAS (A INTELIGÊNCIA DO CRM) */}
            <div style={{ 
              display: 'grid', 
              gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
              gap: '20px', 
              marginBottom: '35px' 
            }}>
              <div style={crmCardStyle}>
                <div style={labelStyle}>Base Total</div>
                <div style={valueStyle}>{leads.length}</div>
                <div style={{ ...subLabelStyle, color: '#5A7FFF' }}>👥 Leads captados</div>
              </div>

              <div style={crmCardStyle}>
                <div style={labelStyle}>Novos (Hoje)</div>
                <div style={valueStyle}>
                  {leads.filter((l: any) => (l.createdAt || l.created_at) && new Date(l.createdAt || l.created_at).toDateString() === new Date().toDateString()).length}
                </div>
                <div style={{ ...subLabelStyle, color: '#10B981' }}>⚡ Escala diária</div>
              </div>

              <div style={crmCardStyle}>
                <div style={labelStyle}>Em Negociação</div>
                <div style={valueStyle}>{leads.filter(l => l.status === 'negociacao').length}</div>
                <div style={{ ...subLabelStyle, color: '#F59E0B' }}>💬 No funil</div>
              </div>

              <div style={crmCardStyle}>
                <div style={labelStyle}>Conversão</div>
                <div style={valueStyle}>
                  {leads.length > 0 ? ((leads.filter(l => l.status === 'concluida').length / leads.length) * 100).toFixed(1) : 0}%
                </div>
                <div style={{ ...subLabelStyle, color: '#A855F7' }}>🎯 Eficiência</div>
              </div>
            </div>

{/* 3. RÉGUA DE STATUS (FLUXO DO FUNIL) */}
            <div style={{ 
              display: 'flex', 
              gap: '12px', 
              marginBottom: '30px',
              overflowX: 'auto',
              paddingBottom: '10px'
            }}>
              <div style={statusBadgeStyle('#334155')}>🤖 Robô: {leads.filter((l: any) => l.status === 'bot').length}</div>
              <div style={statusBadgeStyle('#2563EB')}>🤝 Comercial: {leads.filter((l: any) => l.status === 'negociacao').length}</div>
              <div style={statusBadgeStyle('#10B981')}>✅ Fechados: {leads.filter((l: any) => l.status === 'concluida').length}</div>
              <div style={statusBadgeStyle('#EF4444')}>❌ Perdeu: {leads.filter((l: any) => l.status === 'perdida').length}</div>
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
                              <div
                                className="card"
                                onClick={() => openChat(lead)}
                                ref={provided.innerRef}
                                {...provided.draggableProps}
                                {...provided.dragHandleProps}
                                style={{
                                  ...provided.draggableProps.style,
                                  cursor: 'pointer'
                                }}
                              >
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
                      <div className="kanban-header" style={{ borderTop: '3px solid #f59e0b' }}>🤝 Em Negociação <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsNegociacao.length}</span></div>
                      <div className="kanban-cards">
                        {leadsNegociacao.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div
                                className="card"
                                onClick={() => openChat(lead)}
                                ref={provided.innerRef}
                                {...provided.draggableProps}
                                {...provided.dragHandleProps}
                                style={{ ...provided.draggableProps.style, cursor: 'pointer' }}
                              >
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

                {/* COLUNA: CONCLUÍDA */}
                <Droppable droppableId="concluida">
                  {(provided) => (
                    <div className="kanban-column" ref={provided.innerRef} {...provided.droppableProps}>
                      <div className="kanban-header" style={{ borderTop: '3px solid #10b981' }}>✅ Concluída <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsConcluida.length}</span></div>
                      <div className="kanban-cards">
                        {leadsConcluida.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div
                                className="card"
                                onClick={() => openChat(lead)}
                                ref={provided.innerRef}
                                {...provided.draggableProps}
                                {...provided.dragHandleProps}
                                style={{ ...provided.draggableProps.style, cursor: 'pointer' }}
                              >
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

                {/* COLUNA: PERDIDA */}
                <Droppable droppableId="perdida">
                  {(provided) => (
                    <div className="kanban-column" ref={provided.innerRef} {...provided.droppableProps}>
                      <div className="kanban-header" style={{ borderTop: '3px solid #ef4444' }}>❌ Não Concluída <span style={{ background: '#334155', padding: '2px 8px', borderRadius: '12px', fontSize: '12px' }}>{leadsPerdida.length}</span></div>
                      <div className="kanban-cards">
                        {leadsPerdida.map((lead, idx) => (
                          <Draggable key={String(lead.id)} draggableId={String(lead.id)} index={idx}>
                            {(provided) => (
                              <div
                                className="card"
                                onClick={() => openChat(lead)}
                                ref={provided.innerRef}
                                {...provided.draggableProps}
                                {...provided.dragHandleProps}
                                style={{ ...provided.draggableProps.style, cursor: 'pointer' }}
                              >
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

              </div>
    
    </DragDropContext>
            {selectedLead && (
              <div 
                className="chat-overlay" 
                onClick={() => setSelectedLead(null)}
                style={{ position: 'fixed', top: 0, left: 0, width: '100vw', height: '100vh', backgroundColor: 'rgba(0,0,0,0.7)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 9999 }}
              >
                <div 
                  className="chat-modal" 
                  onClick={e => e.stopPropagation()} 
                  style={{ display: 'flex', flexDirection: 'column', width: '100%', maxWidth: '450px', height: '80vh', maxHeight: '700px', backgroundColor: '#1e293b', borderRadius: '12px', overflow: 'hidden', boxShadow: '0 10px 25px rgba(0,0,0,0.5)' }}
                >
                  
                  {/* CABEÇALHO DO CHAT */}
                  <div className="chat-header" style={{ padding: '15px 20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: '#334155', borderBottom: '1px solid #475569' }}>
                    <div>
                      <strong style={{ fontSize: '16px', color: 'white' }}>{selectedLead.nome || 'Lead'}</strong>
                      <div style={{ fontSize: '13px', color: '#94a3b8', marginTop: '2px' }}>{selectedLead.telefone}</div>
                    </div>
                    <button onClick={() => setSelectedLead(null)} style={{ background: 'none', border: 'none', color: '#94a3b8', cursor: 'pointer', fontSize: '24px' }}>✕</button>
                  </div>
                  
                  {/* CORPO DAS MENSAGENS */}
                  <div className="chat-body" style={{ display: 'flex', flexDirection: 'column', gap: '10px', padding: '20px', overflowY: 'auto', background: '#0f172a', flex: 1 }}>
                    {chatMessages.map((msg, idx) => {
                      // Verifica se é 'in' ou 'inbound' para tratar como recebida do cliente
                      const isReceived = msg.direction === 'in' || msg.direction === 'inbound';
                      
                      return (
                        <div key={idx} className={`chat-bubble ${isReceived ? 'received' : 'sent'}`}>
                          {msg.text}
                        </div>
                      );
                    })}
                    {chatMessages.length === 0 && <p style={{ textAlign: 'center', opacity: 0.5, marginTop: '20px' }}>Sem mensagens.</p>}
                  </div>
                  
                  {/* CORPO DAS MENSAGENS */}
{/* 🚀 ÁREA DE ENVIO DE MENSAGENS */}
<div style={{ padding: '15px', background: '#1e293b', display: 'flex', gap: '10px', borderTop: '1px solid #334155' }}>
  <input
    type="text"
    value={newMessage}
    onChange={(e) => setNewMessage(e.target.value)}
    onKeyDown={(e) => e.key === 'Enter' && handleSendMessage()}
    placeholder="Digite sua mensagem..."
    style={{ 
      flex: 1, 
      padding: '10px', 
      borderRadius: '8px', 
      border: 'none', 
      background: '#0f172a', 
      color: 'white', 
      outline: 'none' 
    }}
  />
  <button 
    onClick={handleSendMessage}
    style={{ 
      padding: '10px 20px', 
      background: '#3b82f6', 
      color: 'white', 
      borderRadius: '8px', 
      border: 'none', 
      cursor: 'pointer', 
      fontWeight: 'bold' 
    }}
  >
    Enviar
  </button>
</div>

{/* 🚀 RODAPÉ DE ENVIO DE MENSAGENS */}
          <div style={{ padding: '15px', background: '#1e293b', display: 'flex', gap: '10px', borderTop: '1px solid #334155' }}>
            <input
              type="text"
              value={newMessage}
              onChange={(e) => setNewMessage(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSendMessage()}
              placeholder="Digite sua mensagem..."
              style={{ flex: 1, padding: '10px', borderRadius: '8px', border: 'none', background: '#0f172a', color: 'white', outline: 'none' }}
            />
            <button 
              onClick={handleSendMessage}
              style={{ padding: '10px 20px', background: '#3b82f6', color: 'white', borderRadius: '8px', border: 'none', cursor: 'pointer', fontWeight: 'bold' }}
            >
              Enviar
            </button>
          </div>



{/* 🚀 NOVO: ÁREA DE ENVIO DE MENSAGENS */}
<div style={{ padding: '15px', background: '#1e293b', display: 'flex', gap: '10px', borderTop: '1px solid #334155' }}>
  <input
    type="text"
    value={newMessage}
    onChange={(e) => setNewMessage(e.target.value)}
    onKeyPress={(e) => e.key === 'Enter' && handleSendMessage()}
    placeholder="Digite sua mensagem..."
    style={{
      flex: 1,
      padding: '12px',
      borderRadius: '8px',
      border: 'none',
      background: '#0f172a',
      color: 'white',
      outline: 'none'
    }}
  />
  <button 
    onClick={handleSendMessage}
    style={{
      padding: '10px 20px',
      background: '#3b82f6',
      color: 'white',
      border: 'none',
      borderRadius: '8px',
      cursor: 'pointer',
      fontWeight: 'bold'
    }}
  >
    Enviar
  </button>
</div>

                  {/* RODAPÉ COM TECLADO E BOTÃO ATIVOS */}
                  <div className="chat-footer" style={{ display: 'flex', padding: '15px', background: '#1e293b', borderTop: '1px solid #334155' }}>
                    <input 
                      type="text" 
                      placeholder="Digite a sua mensagem..." 
                      style={{ flex: 1, padding: '12px', borderRadius: '8px', border: '1px solid #475569', background: '#0f172a', color: 'white', outline: 'none', marginRight: '10px' }}
                      value={newMessage}
                      onChange={(e) => setNewMessage(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') {
                          sendMessage();
                        }
                      }}
                    />
                    <button 
                      onClick={sendMessage}
                      style={{ padding: '0 20px', background: '#2563eb', color: 'white', border: 'none', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold', transition: 'background 0.2s' }}
                      onMouseOver={(e) => e.currentTarget.style.background = '#1d4ed8'}
                      onMouseOut={(e) => e.currentTarget.style.background = '#2563eb'}
                    >
                      Enviar
                    </button>
                  </div>

                </div>
              </div>
            )}
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
          <h3 style={{ marginTop: 0, marginBottom: '20px' }}>✨ Cadastrar Novo Cliente</h3>
          <form onSubmit={handleRegister} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px' }}>
            
            {/* Nomes corrigidos para bater EXATAMENTE com o Backend */}
            <input type="text" name="name" placeholder="Nome da Empresa" className="input-field" required />
            <input type="email" name="email" placeholder="E-mail administrador" className="input-field" required />
            
            {/* Campo novo que o Python exige: */}
            <input type="tel" name="phone" placeholder="Telefone da Empresa" className="input-field" required />
            
            <input type="tel" name="bot_whatsapp" placeholder="WhatsApp do Bot" className="input-field" required />
            <input type="password" name="password" placeholder="Crie uma senha provisória" className="input-field" required />
            
            {/* BOTÃO E FECHAMENTOS QUE ESTAVAM FALTANDO */}
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
                  
                  {/* BOTOES DE AÇÃO LADO A LADO */}
                  <div style={{ display: 'flex', gap: '10px' }}>
                    <button 
                      onClick={() => handleEditCompany(empresa)} 
                      style={{ flex: 1, background: '#3b82f6', color: '#fff', border: 'none', padding: '10px', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}
                    >
                      Editar
                    </button>
                    
                    <button 
                      onClick={() => handleDeleteCompany(empresa.id)} 
                      style={{ flex: 1, background: '#ef4444', color: '#fff', border: 'none', padding: '10px', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}
                    >
                      Apagar
                    </button>
                  </div>

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