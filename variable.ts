
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFActualizarDatosGeneralesPlanTrabajoDto
    {
        public string Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public string FechaEjecucion { get; set; }
        public string Moneda { get; set; }
        public decimal ImporteDetalle { get; set; }
        public decimal ImporteMaestro { get; set; }
        public string NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public string SubtipoOperacion { get; set; }
        public string FolioMaestro { get; set; }
        public string DesAgregado { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFCancelarPlanTrabajoForexDto
    {
        public string Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FechaEjecucion { get; set; }
        public string Moneda { get; set; }
        public decimal Importe { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFCancelarPlanTrabajoMaestroForexDto
    {
        public string FolioMaestro { get; set; }
        public string TipoOperacion { get; set; }
        public string FechaEjecucion { get; set; }
        public string Moneda { get; set; }
        public decimal Importe { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFEntradaSalidaConciliacionDto
    {
        public DateTime EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal EntImporte { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFSumarizadoDetalleTomiDto
    {
        public long id_maestro { get; set; }
        public int num_regs { get; set; }
        public decimal? imp_monto { get; set; }

    }
}
namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFConteoDto
    {
        public int NumRegs { get; set; }
    }
}
namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFDetalleTomiConciliacionDto
    {
        public decimal T24DebitAmount { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefDetalleTomiEntradaSalidaDto
    {
        public string NfFolioUnico { get; set; }

        public string T24CustomerCode { get; set; }

        public string T24CompanyName { get; set; }

        public string T24DebitAcctNo { get; set; }

        public string NfNombreCuenta { get; set; }

        public string NfMonedaCuenta { get; set; }

        public decimal? T24DebitAmount { get; set; }

        public string T24DebitTheirRef { get; set; }

        public int? NfFechaEjecucion { get; set; }
    }
}
namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTEFForexStatusDto
    {
        public string StatusOperacion { get; set; }
    }
}
using System;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefInsertarDetalleTomiDto
    {
        public string NfFolioUnico { get; set; }
        public string NfTipo { get; set; }
        public long NfFolio { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24DebitAcctNo { get; set; }
        public string NfEstatus { get; set; }
        public string NfUsuarioCaptura { get; set; }
        public string NfUsuarioAutoriza { get; set; }
        public DateTime NfFechaCaptura { get; set; }
        public DateTime? NfFechaAutoriza { get; set; }
        public decimal T24DebitAmount { get; set; }
        public string T24ShortName { get; set; }
        public string NfForexRelacionado { get; set; }
        public string T24DebitCurrency { get; set; }
        public string NfBancoBenef { get; set; }
        public string T24BenAcctNo { get; set; }
        public string NfTipoCuenta { get; set; }
        public string T24BenCustName { get; set; }
        public string NfRfcBeneficiario { get; set; }
        public string NfTasaRetencion { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public string T24DebitTheirRef { get; set; }
        public decimal? NfMontoEnviar { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public long? IdFilesBtmum { get; set; }
        public int? NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public int? NoPagosProcesados { get; set; }
        public int? NoPagosDevueltos { get; set; }
        public long? IdFormatoPap { get; set; }
        public string Ref1 { get; set; }
        public string Ref2 { get; set; }
        public string Ref3 { get; set; }
        public string Ref4 { get; set; }
        public string Ref5 { get; set; }
        public string NfCodigoOperacion { get; set; }
        public string NfTipoOperacion { get; set; }
        public int? NfFechaAplicacion { get; set; }
        public int? NfFechaCargo { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefInsertarEntradaSalidaTomiDto
    {
        public string Referencia { get; set; }          
        public int NumCliente { get; set; }             
        public double Cdl { get; set; }                 
        public string Ordenante { get; set; }           

        public double Operacion { get; set; }           
        public string OperacionDesc { get; set; }       
        public string Tipo { get; set; }                
        public string TipoDesc { get; set; }            

        public double Cuenta { get; set; }              
        public string Nombre { get; set; }              
        public string CuentaMoneda { get; set; }        

        public double Via { get; set; }                 
        public string ViaDesc { get; set; }             

        public decimal Importe { get; set; }            
        public decimal Saldo { get; set; }              
        public string Comentarios { get; set; }         

        public DateTime FechaCaptura { get; set; }      
        public DateTime FechaValor { get; set; }        
        public string Hora { get; set; }                

        public string Capturista { get; set; }          
        public double Fad { get; set; }                 
        public string ReferenciaControl { get; set; }   
        public double ReferenciaControlCont { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefInsertarMaestroTomiDto
    {
        public string NfFolioUnico { get; set; }          
        public string NfTipo { get; set; }                
        public double NfFolio { get; set; }               
        public string T24CustomerCode { get; set; }       
        public string T24DebitAcctNo { get; set; }        
        public string NfEstatus { get; set; }             
        public string NfUsuarioCaptura { get; set; }      
        public string NfUsuarioAutoriza { get; set; }     
        public DateTime NfFechaCaptura { get; set; }      
        public DateTime? NfFechaAutoriza { get; set; }    
        public decimal T24DebitAmount { get; set; }        

        public string T24ShortName { get; set; }          
        public string NfForexRelacionado { get; set; }    
        public string T24DebitCurrency { get; set; }      
        public string NfBancoBenef { get; set; }          
        public string T24BenAcctNo { get; set; }          
        public string NfTipoCuenta { get; set; }          
        public string T24BenCustName { get; set; }        
        public string NfRfcBeneficiario { get; set; }     
        public string NfTasaRetencion { get; set; }       
        public int NfFechaEjecucion { get; set; }         
        public string T24DebitTheirRef { get; set; }      
        public decimal NfMontoEnviar { get; set; }         

        public string IdPlantilla { get; set; }           
        public double IdFormatoPlantilla { get; set; }    
        public double IdFilesBtmum { get; set; }          
        public double NoPagos { get; set; }               
        public string PagosDescripcion { get; set; }      
        public double NoPagosProcesados { get; set; }     
        public double NoPagosDevueltos { get; set; }      

        public double NfCodigoOperacion { get; set; }     
        public double NfTipoOperacion { get; set; }       
        public int NfFechaAplicacion { get; set; }        
        public int NfFechaCargo { get; set; }             
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefResultadoSalidaDto
    {
        public short Salida { get; set; }

        public bool EsExitoso
        {
            get { return Salida == 0; }
        }
    }
}
using System;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PapTefContenidoFormatoDto
    {
        public string Valor00 { get; set; }
        public string Valor01 { get; set; }
        public string Valor02 { get; set; }
        public string Valor03 { get; set; }
        public string Valor04 { get; set; }
        public string Valor05 { get; set; }
        public string Valor06 { get; set; }
        public string Valor07 { get; set; }
        public string Valor08 { get; set; }
        public string Valor09 { get; set; }
        public string Valor10 { get; set; }
        public string Valor11 { get; set; }
        public string Valor12 { get; set; }
        public string Valor13 { get; set; }
        public string Valor14 { get; set; }
        public string Valor15 { get; set; }
        public string Valor16 { get; set; }
        public string Valor17 { get; set; }
        public string Valor18 { get; set; }

        public string this[int index]
        {
            get
            {
                switch (index)
                {
                    case 0: return Valor00;
                    case 1: return Valor01;
                    case 2: return Valor02;
                    case 3: return Valor03;
                    case 4: return Valor04;
                    case 5: return Valor05;
                    case 6: return Valor06;
                    case 7: return Valor07;
                    case 8: return Valor08;
                    case 9: return Valor09;
                    case 10: return Valor10;
                    case 11: return Valor11;
                    case 12: return Valor12;
                    case 13: return Valor13;
                    case 14: return Valor14;
                    case 15: return Valor15;
                    case 16: return Valor16;
                    case 17: return Valor17;
                    case 18: return Valor18;
                    default: throw new IndexOutOfRangeException($"Índice inválido para contenidoFormato: {index}");
                }
            }
            set
            {
                switch (index)
                {
                    case 0: Valor00 = value; break;
                    case 1: Valor01 = value; break;
                    case 2: Valor02 = value; break;
                    case 3: Valor03 = value; break;
                    case 4: Valor04 = value; break;
                    case 5: Valor05 = value; break;
                    case 6: Valor06 = value; break;
                    case 7: Valor07 = value; break;
                    case 8: Valor08 = value; break;
                    case 9: Valor09 = value; break;
                    case 10: Valor10 = value; break;
                    case 11: Valor11 = value; break;
                    case 12: Valor12 = value; break;
                    case 13: Valor13 = value; break;
                    case 14: Valor14 = value; break;
                    case 15: Valor15 = value; break;
                    case 16: Valor16 = value; break;
                    case 17: Valor17 = value; break;
                    case 18: Valor18 = value; break;
                    default: throw new IndexOutOfRangeException($"Índice inválido para contenidoFormato: {index}");
                }
            }
        }
    }
}
using System;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefDetalleCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long? IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXMLDet { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public int? FechaCargo { get; set; }
        public int? FechaAplicacion { get; set; }
        public DateTime? FechaAutoriza { get; set; }
        public string IdUsuarioAutorizoDos { get; set; }
        public DateTime? FechaAutorizacionDos { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdAutorizaciones { get; set; }
        public int? IdAutorizacionUno { get; set; }
        public int? IdAutorizacionDos { get; set; }
        public long? Estilo { get; set; }
        public long? AutorizacionTOMITAS { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string IdBeneficiario { get; set; }
        public long IdFilesBtmum { get; set; }
        public long NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public long NoPagosProcesados { get; set; }
        public long NoPagosDevueltos { get; set; }
        public long IdFormatoPap { get; set; }
        public string CodigoOperacion { get; set; }
        public string TipoOperacion { get; set; }
        public string Ref1 { get; set; }
        public string Ref2 { get; set; }
        public string Ref3 { get; set; }
        public string Ref4 { get; set; }
        public string Ref5 { get; set; }
        public string IdCanal { get; set; }

        // LEFT JOIN cliente B ON A.id_cliente = B.id_cliente
        // B.id_cte_t24 AS cdl
        public decimal Cdl { get; set; }
    }
}
using System;

namespace Aplicacion.DTOs.PaP
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class PaPTefMaestroCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long? IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXMLDet { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public int? FechaCargo { get; set; }
        public int? FechaAplicacion { get; set; }
        public DateTime? FechaAutoriza { get; set; }
        public string IdUsuarioAutorizoDos { get; set; }
        public DateTime? FechaAutorizacionDos { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdAutorizaciones { get; set; }
        public int? IdAutorizacionUno { get; set; }
        public int? IdAutorizacionDos { get; set; }
        public long? Estilo { get; set; }
        public long? AutorizacionTOMITAS { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string IdBeneficiario { get; set; }
        public long IdFilesBtmum { get; set; }
        public long NoPagos { get; set; }
        public string PagosDescripcion { get; set; }
        public long NoPagosProcesados { get; set; }
        public long NoPagosDevueltos { get; set; }
        public string CodigoOperacion { get; set; }
        public string TipoOperacion { get; set; }
        public string IdCanal { get; set; }
        public decimal Cdl { get; set; }
    }

}
using Aplicacion.Modelos.SAT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SAT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class CONTEO_DIAS_INHABIL_SAT_DTO
    {
        public int Conteo {  get; set; }
    }
    /// <summary>
    /// Representa SAT Datos Regreso Modelo.
    /// </summary>
    public class SAT_DatosRegresoModelo
    {
        public bool Resultado { get; set; }
        public int ResultadoValidacion { get; set; }
        public SAT_ValoresFormato_Modelo ValoresRegreso { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SAT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SALIDAAPPLET_UTILERIAS_SAT_DTO
    {
        public string NF_FOLIO { get; set; }
        public string CLAVE { get; set; }
        public string VALOR { get; set; }
        public string VALOR_R { get; set; }
        public string Grupo { get; set; }
        public string Renglon { get; set; }
        public string Orden { get; set; }
        public string OrdenArchivo { get; set; }
        public int Conteo { get; set; } = 0;
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SAT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SAT_DTO
    {
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public decimal Cdl { get; set; }
        public long Id_cuenta { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SAT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SAT_Formato_DTO
    {
        public long Id_Formato { get; set; }
        public long Id_Cliente { get; set; }
        public string Id_UsuarioCaptura { get; set; }
        public long Id_Cuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime? FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string Id_UsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string ContenidoSelloDigit { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string FormatoOriginalCompleto { get; set; }
        public string IPOrigen { get; set; }
        public string CadenaOriginal { get; set; }
        public string Mensaje { get; set; }
        public string CadenaOriginalApplet { get; set; }
        public string FormatoXML { get; set; }
        public string FormatoXML_Det { get; set; }
        public string SerieCertificado { get; set; }
        public string Descripcion { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? Fecha_Autoriza { get; set; }
        public string Id_UsuarioAutorizo_dos { get; set; }
        public DateTime? FechaAutorizacion_dos { get; set; }
        public string Id_Plantilla { get; set; }
        public long Id_Autorizaciones { get; set; }
        public int Id_Autorizacion_uno { get; set; }
        public int Id_Autorizacion_dos { get; set; }
        public long Estilo { get; set; }
        public string Autorizacion_TOMITAS { get; set; }
        public long Id_Formato_Plantilla { get; set; }
        public string Id_Beneficiario { get; set; }
        public string Id_CodigoMensaje { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SAT
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SAT_HdClientes_DTO
    {
        public string Satm02nom {  get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SER
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerFormatoCmsDto
    {
        public long IdFormat { get; set; }
        public decimal? Cdl { get; set; }
        public long? IdAccount { get; set; }

        public DateTime CaptureDate { get; set; }
        public string IdUserCapture { get; set; }

        public DateTime? AuthorizationDate { get; set; }
        public string IdAuthorizationUser { get; set; }

        public decimal OperatingAmount { get; set; }
        public string IdTemplate { get; set; }
        public long? IdTemplateFormat { get; set; }

        public string ContentFormat { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerActualizarPlanTrabajoMaestroDto
    {
        public long Folio { get; set; }
        public string FolioUnico { get; set; }

        public int? FechaValor { get; set; }

        public string CveMoneda { get; set; }

        // money
        public decimal? Importe { get; set; }

        public int? NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public string SubtipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerInsertarSerPayDto
    {
        public string NfTipo { get; set; }
        public long NfFolio { get; set; }

        public string T24CustomerCode { get; set; }
        public string T24CompanyName { get; set; }
        public string T24ShortName { get; set; }

        public string NfEstatus { get; set; }

        public string NfUsuarioCaptura { get; set; }
        public DateTime? NfFechaCaptura { get; set; }

        public string NfUsuarioAutoriza { get; set; }
        public DateTime? NfFechaAutoriza { get; set; }

        public int? NfFechaEjecucion { get; set; }

        public string T24BenCustName { get; set; }
        public string T24NoAgreement { get; set; }
        public string T24Currency { get; set; }
        public string T24Reference { get; set; }

        public decimal? T24Amount { get; set; }

        public string T24Concept { get; set; }
        public string T24AcctNo { get; set; }

        public decimal? NfMontoEnviar { get; set; }
        public string NfFolioUnico { get; set; }

        public long? IdFormatoPlantilla { get; set; }

        public string NfTipoCuenta { get; set; }
        public string NfNombreCuenta { get; set; }
        public string NfMonedaCuenta { get; set; }

        public string IdPlantilla { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerPayEntradaSalidaDto
    {
        public string NfFolioUnico { get; set; }

        public string T24CustomerCode { get; set; }
        public string T24CompanyName { get; set; }

        public string T24AcctNo { get; set; }
        public string NfNombreCuenta { get; set; }
        public string NfMonedaCuenta { get; set; }

        public decimal? T24Amount { get; set; }
        public string T24Concept { get; set; }

        public int? NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerInsertarEntradaSalidaDto
    {
        public string Referencia { get; set; }

        public decimal NumCliente { get; set; }
        public decimal Cdl { get; set; }

        public string Ordenante { get; set; }

        public decimal Operacion { get; set; }
        public string OperacionDesc { get; set; }

        public string Tipo { get; set; }
        public string TipoDesc { get; set; }

        public decimal? Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }

        public decimal Via { get; set; }
        public string ViaDesc { get; set; }

        public decimal Importe { get; set; }
        public decimal Saldo { get; set; }

        public string Comentarios { get; set; }

        public DateTime? FechaCaptura { get; set; }
        public DateTime? FechaValor { get; set; }

        public string Hora { get; set; }
        public string Capturista { get; set; }

        public decimal? Fad { get; set; }

        public string ReferenciaControl { get; set; }

        public int? ReferenciaControlCont { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerPayConciliacionDto
    {
        public string T24Currency { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public decimal? T24Amount { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerEntradaSalidaConciliacionDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal EntImporte { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerConteoDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SerSalidaDto
    {
        public int Salida { get; set; }
    }

}

namespace Aplicacion.DTOs.SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPIDLocalDtos
    {
        /// <summary>
        /// DTO necesario para recuperar datos.
        /// </summary>
        public class SpidLocalDatosBasicosDto
        {
            public string NfFolioUnico { get; set; }
            public int? NfFechaEjecucion { get; set; }
            public string NfDebitAcctNo { get; set; }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPID_DiasNoHabiles_DTO
    {
        public DateTime? DIANOHABIL {  get; set; }
        public string TIPOCCY { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPID_InsightDireccion_DTO
    {
        public string NIC { get; set; }
        public string P1_CALLE { get; set; }
        public string P1_EXTERIOR { get; set; }
        public string P1_INTERIOR { get; set; }
        public string P1_CODIGO_POSTAL { get; set; }
    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPID_ForexBancosFax_DTO
    {
        public string STATUSOPERACION {  get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPID_Formato_DTO
    {
        public long Id_Formato { get; set; }
        public long Id_Cliente { get; set; }
        public decimal Cdl { get; set; }
        public string Id_UsuarioCaptura { get; set; }
        public long Id_Cuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime? FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string Id_UsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string IPOrigen { get; set; }
        public string Mensaje { get; set; }
        public string FormatoXML { get; set; }
        public DateTime? FechaEjecucion { get; set; }
        public DateTime? Fecha_Autoriza { get; set; }
        public string Id_UsuarioAutorizo_dos { get; set; }
        public DateTime? FechaAutorizacion_dos { get; set; }
        public string Id_Plantilla { get; set; }
        public long Id_Autorizaciones { get; set; }
        public int Id_Autorizacion_uno { get; set; }
        public int Id_Autorizacion_dos { get; set; }
        public long Estilo { get; set; }
        public string Autorizacion_TOMITAS { get; set; }
        public long Id_Formato_Plantilla { get; set; }
        public string Id_Beneficiario { get; set; }
        public string ClasificacionOper { get; set; }
        public string RelacionComercial { get; set; }
        public string ReferenciaNumerica { get; set; }
        public string Id_CodigoMensaje { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPID_Tombin_ES_DTO
    {
        public int Num_Regs {  get; set; }
        public string Ent_fechaValor {  get; set; }
        public string Ent_moneda_cuenta {  get; set; }
        public decimal? Ent_importe {  get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SPID
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SPID_Tombin_NF_DTO
    {
        public int Num_regs { get; set; }
        public string NF_FOLIO_UNICO {  get; set; }
        public string T24_CUSTOMER_CODE {  get; set; }
        public string T24_COMPANY_NAME {  get; set; }
        public string T24_DEBIT_ACCT_NO {  get; set; }
        public string NF_NOMBRE_CUENTA {  get; set; }
        public string NF_MONEDA_CUENTA { get; set; }
        public decimal T24_DEBIT_AMOUNT {  get; set; }
        public string T24_DEBIT_THEIR_REF {  get; set; }
        public string NF_FOREX_RELACIONADO {  get; set; }
        public string T24_DEBIT_CURRENCY {  get; set; }
        public int NF_FECHA_EJECUCION {  get; set; }
    }
}

namespace Aplicacion.DTOs.SPID_GPH
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidGphDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public string T24DebitAcctNo { get; set; }
        public string T24CustomerCode { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidGphInsightDireccionDto
    {
        public string NIC { get; set; }
        public string P1CALLE { get; set; }
        public string P1EXTERIOR { get; set; }
        public string P1INTERIOR { get; set; }
        public string P1CODIGOPOSTAL { get; set; }
    }

    public class ValidacionCuentaSpidResultado
    {
        public bool EsValida { get; set; }
        public string ErrorDescripcion { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.SPID_H2H
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidH2hDatosOperacionDto
    {
        public string FolioUnico { get; set; }
        public int? FechaEjecucion { get; set; }
        public string DebitAcctNo { get; set; }
        public string CustomerCode { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidH2hActualizarPlanTrabajoMaestroDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public int? FechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidH2hMarcarPlanTrabajoConErrorDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string ErrorDescripcion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidH2hActualizarDireccionDto
    {
        public long Folio { get; set; }
        public string Address { get; set; }
        public string ZipCode { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidH2hDiaNoHabilDto
    {
        public DateTime DiaNoHabil { get; set; }
        public string TipoCcy { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SpidH2hCustomerEdoCtaDto
    {
        public string NIC { get; set; }
        public string P1_CALLE { get; set; }
        public string P1_EXTERIOR { get; set; }
        public string P1_INTERIOR { get; set; }
        public string P1_CODIGO_POSTAL { get; set; }
    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.TAX
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TaxFormatoCmsDto
    {
        public long IdFormat { get; set; }
        public decimal? Cdl { get; set; }
        public long? IdAccount { get; set; }
        public DateTime CaptureDate { get; set; }
        public string IdUserCapture { get; set; }
        public DateTime? AuthorizationDate { get; set; }
        public string IdAuthorizationUser { get; set; }
        public decimal OperatingAmount { get; set; }
        public string IdTemplate { get; set; }
        public long? IdTemplateFormat { get; set; }
        public string ContentFormat { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TaxPayEntradaSalidaDto
    {
        public string NfFolioUnico { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24CompanyName { get; set; }
        public string T24AcctNo { get; set; }
        public string NfNombreCuenta { get; set; }
        public string NfMonedaCuenta { get; set; }
        public decimal? T24Amount { get; set; }
        public string T24Concept { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TaxPayConciliacionDto
    {
        public string T24Currency { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public decimal? T24Amount { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TaxPayConteoDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class ResultadoOperacionDto
    {
        public short Salida { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TaxEntradaSalidaConciliacionDto
    {
        public DateTime? EntFechaValor { get; set; }
        public string EntMonedaCuenta { get; set; }
        public double? EntImporte { get; set; }
    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.TEF
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefFormatoCmsDto
    {
        public long IdFormato { get; set; }
        public long IdCliente { get; set; }
        public string IdUsuarioCaptura { get; set; }
        public long IdCuenta { get; set; }
        public string Estatus { get; set; }
        public DateTime FechaCaptura { get; set; }
        public DateTime? FechaAutorizacion { get; set; }
        public string IdUsuarioAutorizo { get; set; }
        public string ContenidoFormato { get; set; }
        public string RazonRechazo { get; set; }
        public long TipoFormato { get; set; }
        public decimal MontoOperacion { get; set; }
        public string Mensaje { get; set; }
        public DateTime FechaEjecucion { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public int FechaAplicacionAbono { get; set; }
        public int FechaCargoFondos { get; set; }
        public string CodigoOperacion { get; set; }
        public string TipoOperacion { get; set; }
        public decimal? Cdl { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefForexStatusTomiDto
    {
        public string StatusOperacion { get; set; }
    }


    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefExisteFormatoTomiDto
    {
        public int NumRegs { get; set; }
    }


    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefConciliacionTomiDto
    {
        public string NfForexRelacionado { get; set; }
        public string T24DebitCurrency { get; set; }
        public int? NfFechaEjecucion { get; set; }
        public decimal? T24DebitAmount { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefInsertarFormatoTomiDto
    {
        public string NfFolioUnico { get; set; }
        public string NfTipo { get; set; }
        public long NfFolio { get; set; }
        public string T24CustomerCode { get; set; }
        public string T24DebitAcctNo { get; set; }
        public string NfEstatus { get; set; }
        public string NfUsuarioCaptura { get; set; }
        public string NfUsuarioAutoriza { get; set; }
        public DateTime NfFechaCaptura { get; set; }
        public DateTime? NfFechaAutoriza { get; set; }
        public decimal T24DebitAmount { get; set; }
        public string T24ShortName { get; set; }
        public string NfForexRelacionado { get; set; }
        public string T24DebitCurrency { get; set; }
        public string NfBancoBenef { get; set; }
        public string T24BenAcctNo { get; set; }
        public string NfTipoCuenta { get; set; }
        public string T24BenCustName { get; set; }
        public string NfRfcBeneficiario { get; set; }
        public string NfTasaRetencion { get; set; }
        public int NfFechaEjecucion { get; set; }
        public string T24DebitTheirRef { get; set; }
        public decimal NfMontoEnviar { get; set; }
        public string IdPlantilla { get; set; }
        public long? IdFormatoPlantilla { get; set; }
        public string NfCodigoOperacion { get; set; }
        public string NfTipoOperacion { get; set; }
        public int NfFechaAplicacion { get; set; }
        public int NfFechaCargo { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefInsertarEntradaSalidaTomiDto
    {
        public string EntReferencia { get; set; }
        public decimal EntCliente { get; set; }
        public decimal EntCdl { get; set; }
        public string EntOrdenante { get; set; }
        public decimal EntOperacion { get; set; }
        public string EntOperacionDesc { get; set; }
        public string EntTipo { get; set; }
        public string EntTipoDesc { get; set; }
        public decimal? EntCuenta { get; set; }
        public string EntNombreCuenta { get; set; }
        public string EntMonedaCuenta { get; set; }
        public decimal EntViaPago { get; set; }
        public string EntViaPagoDesc { get; set; }
        public decimal EntImporte { get; set; }
        public decimal EntSaldo { get; set; }
        public string EntComentarios { get; set; }
        public DateTime? EntFechaCaptura { get; set; }
        public DateTime? EntFechaValor { get; set; }
        public string EntHora { get; set; }
        public string EntCaptura { get; set; }
        public decimal? EntFad { get; set; }
        public string EntReferenciaControl { get; set; }
        public int? EntReferenciaControlCont { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefActualizarDatosGeneralesPlanTrabajoDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public decimal? FecValor { get; set; }
        public string CveMoneda { get; set; }
        public decimal? Importe { get; set; }
        public int? NumCliente { get; set; }
        public string NombreCliente { get; set; }
        public string NumCuenta { get; set; }
        public string SubtipoOperacion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefCancelarPlanTrabajoPorForexDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public decimal? FecValor { get; set; }
        public string CveMoneda { get; set; }
        public decimal? Importe { get; set; }
        public string ErrDescripcion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefActualizarEstatusCmsDto
    {
        public long IdFormato { get; set; }
        public string Mensaje { get; set; }
        public string Estatus { get; set; }
        public string RazonRechazo { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefActualizarEstatusTomiDto
    {
        public long NfFolio { get; set; }
        public string NfEstatus { get; set; }
        public string NfRazon { get; set; }
        public string NfUsuarioConfirma { get; set; }
        public DateTime? NfFechaConfirma { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefEntradaSalidaOrigenTomiDto
    {
        public string NfFolioUnico { get; set; }

        public string T24CustomerCode { get; set; }

        public string T24CompanyName { get; set; }

        public string NfForexRelacionado { get; set; }

        public string T24DebitAcctNo { get; set; }

        public string NfNombreCuenta { get; set; }

        public string NfMonedaCuenta { get; set; }

        public decimal? T24DebitAmount { get; set; }

        public string T24DebitTheirRef { get; set; }

        public int? NfFechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefExisteEntradaSalidaTomiDto
    {
        public int NumRegs { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefConciliacionEntradaSalidaTomiDto
    {
        public DateTime? EntFechaValor { get; set; }

        public string EntMonedaCuenta { get; set; }

        public decimal? EntImporte { get; set; }
    }

}

namespace Aplicacion.DTOs.TEF_GPH
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefGphDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.TEF_H2H
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefH2hDatosOperacionDto
    {
        public string FolioUnico { get; set; }
        public int? FechaEjecucion { get; set; }
    }

    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefH2hActualizarPlanTrabajoMaestroDto
    {
        public long Folio { get; set; }
        public string TipoOperacion { get; set; }
        public string FolioUnico { get; set; }
        public int? FechaEjecucion { get; set; }
        public string MonedaDefaultMN { get; set; }
    }
}

namespace Aplicacion.DTOs.TEF_Local
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TefLocalDatosBasicosDto
    {
        public string NfFolioUnico { get; set; }
        public int? NfFechaEjecucion { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.TMD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TMD_Formato_DTO
    {
        public long Id_Formato { get; set; }
        public long Id_Cliente { get; set; }
        public decimal Cdl {  get; set; }
        public long Id_Cuenta { get; set; }
        public string MonedaDestino { get; set; }
        public decimal MontoOperacion { get; set; }
        public int Plazo { get; set; }
        public DateTime FechaVencimiento { get; set; }
        public decimal EC_Rate { get; set; }
        public decimal Spread { get; set; }
        public decimal Tasa { get; set; }
        public decimal Tasa_retencion { get; set; }
        public decimal Interes { get; set; }
        public decimal ISR { get; set; }
        public decimal MontoProyectado { get; set; }
        public string ConceptoPago { get; set; }
        public string Estatus { get; set; }
        public string IPOrigen { get; set; }
        public long Id_Autorizaciones { get; set; }
        public string Id_UsuarioCaptura { get; set; }
        public DateTime FechaCaptura { get; set; }
        public string Id_UsuarioAutorizo { get; set; }
        public DateTime FechaAutorizacion { get; set; }
        public string Id_UsuarioAutorizo_dos { get; set; }
        public DateTime FechaAutorizacion_dos { get; set; }
        public int Id_Autorizacion_uno { get; set; }
        public int Id_Autorizacion_dos { get; set; }
        public int Estilo { get; set; }
        public int TipoFormato { get; set; }
        public string ContenidoFormato { get; set; }
        public string Mensaje { get; set; }
        public string FormatoXML { get; set; }
        public string RazonRechazo { get; set; }
        public DateTime Fecha_inicio { get; set; }
        public string Id_tasa_especial { get; set; }
        
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.TMD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TMD_NF_DTO
    {
        public int Num_regs { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs.TMD
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class TMD_Solicitudes_DTO
    {
        public string Estatus { get; set; }
        public string EstatusOperacion { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class RespuestaErrorDto
    {
        public long Folio { get; set; }
        public string Codigo { get; set; }
        public string Mensaje { get; set; }
        public bool Reintentable { get; set; }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class RespuestaPaqueteDto
    {
        public int Version { get; set; } = 1;
        public string Servicio { get; set; }
        public string Tipo { get; set; }

        public int Resultado { get; set; } // 1 OK | 2 PARCIAL | 3 ERROR

        public RespuestaResumenDto Resumen { get; set; }
        public RespuestaTiempoDto Tiempos { get; set; }

        public List<RespuestaErrorDto> Errores { get; set; } = new List<RespuestaErrorDto>();

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class RespuestaResumenDto
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase RespuestaResumenDto.
        /// </summary>
        public RespuestaResumenDto() { }
        /// <summary>
        /// Inicializa una nueva instancia de la clase RespuestaResumenDto.
        /// </summary>
        public RespuestaResumenDto(int total)
        {
            Total = total;
        }

        public int Total { get; set; }
        public int Procesadas { get; set; } = 0;
        public int Ok { get; set; } = 0;
        public int Error { get; set; } = 0;
        public bool FallaCritica { get; set; } = false;
        public string MensajeFallaCritica { get; set; }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public int CalcularResultado()
        {
            if (FallaCritica)
                return 3; // ERROR

            if (Error == 0)
                return 1; // OK

            if (Ok > 0)
                return 2; // PARCIAL

            return 3;     // ERROR


        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class RespuestaTiempoDto
    {
        public DateTime Inicio { get; set; }
        public DateTime Fin { get; set; }
        public long DuracionMs =>
            (long)(Fin - Inicio).TotalMilliseconds;

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SolicitudOperacionDto
    {
        public long Folio { get; set; }

        public string TipoOperacion { get; set; }
    }
}

using Aplicacion.DTOs.ConfiguracionServicio;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.DTOs
{
    /// <summary>
    /// DTO necesario para recuperar datos.
    /// </summary>
    public class SolicitudPaqueteDto
    {
        public string Servicio { get; set; }
        public string Tipo { get; set; }
        public long PID { get; set; }
        public DateTime EnviadoEn { get; set; }
        public IEnumerable<SolicitudOperacionDto> Operaciones { get; set; }
        public int Version { get; set; } = 1;

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos.SAT
{
    /// <summary>
    /// Representa SAT Tombin CMS Modelo.
    /// </summary>
    public class SAT_Tombin_CMS_Modelo
    {
        public int num_regs {  get; set; }
        public decimal T24_DEBIT_AMOUNT { get; set; }
        public string NF_FECHA_EJECUCION { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos.SAT
{
    /// <summary>
    /// Representa SAT Valores Formato Modelo.
    /// </summary>
    public class SAT_ValoresFormato_Modelo
    {
        public string ArchivoSat { get; set; }
        public string CadenaOriginal { get; set; }
        public string SelloDigital { get; set; }
        public string LLavePago { get; set; }
        public string FechaPresentacion { get; set; }
        public string FechaRecaudacion { get; set; }
        public int FechaEjecucion { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos.SPID
{
    /// <summary>
    /// Representa SPID Insertar Entradas Salidas Modelo.
    /// </summary>
    public class SPID_InsertarEntradasSalidasModelo
    {
        public string Referencia { get; set; }
        public decimal? Num_Cliente { get; set; }
        public decimal? CDL { get; set; }
        public string Ordenante { get; set; }
        public decimal? Operacion { get; set; }
        public string OperacionDesc { get; set; }
        public string Tipo { get; set; }
        public string TipoDesc { get; set; }
        public decimal? Cuenta { get; set; }
        public string Nombre { get; set; }
        public string CuentaMoneda { get; set; }
        public decimal? Via { get; set; }
        public string ViaDesc { get; set; }
        public decimal? Importe { get; set; }
        public decimal? Saldo { get; set; }
        public string Comentarios { get; set; }
        public DateTime? Fecha_Captura { get; set; }
        public DateTime? FechaValor { get; set; }
        public string Hora { get; set; }
        public string Capturista { get; set; }
        public decimal? Fad { get; set; }
        public string Referencia_Control { get; set; }
        public int Referencia_Control_Cont { get; set; }
        public int Salida { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos.SPID
{
    /// <summary>
    /// Representa SPID Insertar Formato Modelo.
    /// </summary>
    public class SPID_InsertarFormatoModelo
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_CUSTOMER_CODE { get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime? NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal? T24_DEBIT_AMOUNT { get; set; }
        public string T24_SHORT_NAME { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_DEBIT_CURRENCY { get; set; }
        public string NF_BANCO_BENEF { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_TIPO_CUENTA { get; set; }
        public string T24_BEN_CUST_NAME { get; set; }
        public string NF_RFC_BENEFICIARIO { get; set; }
        public string NF_TASA_RETENCION { get; set; }
        public int NF_FECHA_EJECUCION { get; set; }
        public string T24_DEBIT_THEIR_REF { get; set; }
        public decimal? NF_MONTO_ENVIAR { get; set; }
        public string ID_PLANTILLA { get; set; }
        public long ID_FORMATO_PLANTILLA { get; set; }
        public string NF_ADDRESS { get; set; }
        public string NF_ZIP_CODE { get; set; }
        public string NF_CLASIFICACION_OPERACION { get; set; }
        public string NF_RELACION_COMERCIAL { get; set; }
        public string NF_NUMERIC_REFERENCE { get; set; }
        public string NF_DIRECCION_IP { get; set; }
        public int Salida { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos.TMD
{
    /// <summary>
    /// Representa TMD Insertar Formato Modelo.
    /// </summary>
    public class TMD_InsertarFormatoModelo
    {
        public string NF_FOLIO_UNICO { get; set; }
        public string NF_TIPO { get; set; }
        public long NF_FOLIO { get; set; }
        public string T24_CUSTOMER_ID { get; set; }
        public string T24_DRAWDOWN_ACCOUNT { get; set; }
        public string NF_ESTATUS { get; set; }
        public string NF_USUARIO_CAPTURA { get; set; }
        public string NF_USUARIO_AUTORIZA { get; set; }
        public DateTime NF_FECHA_CAPTURA { get; set; }
        public DateTime? NF_FECHA_AUTORIZA { get; set; }
        public decimal T24_DRAWDOWN_NET_AMT { get; set; }
        public string NF_FOREX_RELACIONADO { get; set; }
        public string T24_CURRENCY { get; set; }
        public int T24_VALUE_DATE { get; set; }
        public int T24_FIN_MAT_DATE { get; set; }
        public double T24_INTEREST_RATE { get; set; }
        public double T24_INTEREST_SPREAD { get; set; }
        public int Salida { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos.TMD
{
    /// <summary>
    /// Representa TMD Tombin Modelo.
    /// </summary>
    public class TMD_Tombin_Modelo
    {
        public string NF_FOREX_RELACIONADO {  get; set; }
        public string T24_CURRENCY { get; set; }
        public int NF_FECHA_EJECUCION {  get; set; }
        public decimal T24_DRAWDOWN_NET_AMT { get; set; }
        public string NF_ESTATUS { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Modelo con la estructura para generar firma digital.
    /// </summary>
    public class ComunFXD_GFDModelo
    {
        public int NF_FECHA_EJECUCION {  get; set; }
        public string NF_BANCO_BENEF {  get; set; }
        public string T24_TRANSACTION_ID { get; set; }
        public decimal IMPORTE {  get; set; }
        public string T24_DEBIT_ACCT_NO { get; set; }
        public string T24_BEN_ACCT_NO { get; set; }
        public string NF_FOREX_RELACIONADO {  get; set; }
        public string T24_DEBIT_THEIR_REF {  get; set; }
        public string NF_FOLIO_UNICO {  get; set; }
        public string CASFIM_MUFG { get; set; }
        public string[] LLAVE {  get; set; }

    }
}

using MUFG_T24.Configuracion_utilidades;
using System;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa Configuracion Modelo.
    /// </summary>
    public class ConfiguracionModelo : Configuracion
    {

        /// <summary>
        /// Propiedades generales del servicio.
        /// </summary>    
        public string StrDespachador_Puerto { get; set; } = null;
        public int StrPID { get; set; } = 0;
        public string StrNombreServicio { get; set; } = null;
        public string StrDisplayName { get; set; } = null;
        public string StrFechaSistema { get; set; } = null;
        public TimeSpan? StrFechaSistemaJuliana { get; set; } = null;
        public TimeSpan? StrHoraCierreSAT { get; set; } = null;
        public TimeSpan? StrHoraLecturaInicio { get; set; } = null;
        public TimeSpan? StrHoraLecturaFin { get; set; } = null;
        /// <summary>
        /// Datos para el uso del WEB SERVICE
        /// </summary>
        public TimeSpan? StrFechaSistema_T24_Juliana { get; set; } = null;
        public string StrWSCompany { get; set; } = null;
        public string StrWSRegisterUserName { get; set; } = null;
        public string StrWSRegisterPassword { get; set; } = null;
        public string StrWSColumnName { get; set; } = null;
        public string StrWSOperationID { get; set; } = null;
        /// <summary>
        /// Datos Servidor SAT
        /// </summary>
        public string StrValidadorSAT_Servidor { get; set; } = null;
        public string StrValidadorSAT_Puerto { get; set; } = null;
        public string StrValidadorSAT_Ruta { get; set; } = null;
        public string StrValidadorSAT_IgnoraValidador { get; set; } = null;
        public string StrValidadorSAT_CertPathName { get; set; } = null;
        public string StrValidadorSAT_CertPsw { get; set; } = null;
        public string StrValidadorSAT_NoSerieCertKarPay { get; set; } = null;
        public bool? BoRequiredTownNameForPaP { get; set; } = false;


        public bool BHorarioLectura { get; set; } = false;
        public bool? BTimerEnabled { get; set; } = null;
        public double? DTimerInterval { get; set; } = null;
        public bool? BoEsDiaHabil { get; set; } = false;
        public bool? BenHorario { get; set; } = false;
        public object StrRutaArchivoConfiguracion { get; set; }


        public string RutaPFX { get; set; } = null;
        public string SerialKarpay { get; set; } = null;
        public string SerialPassword { get; set; } = null;
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public new ConfiguracionModelo Clone()
        {
            /// <summary>
            /// Ejecuta la operaciÃ³n return.
            /// </summary>
            return (ConfiguracionModelo)MemberwiseClone();
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Modelos
{
    /// <summary>
    /// Representa Result.
    /// </summary>
    public class Result<T>
    {
        public bool IsSuccess { get; set; }
        public string Reason { get; set; } = string.Empty;
        public T Data { get; set; }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public static Result<T> Success(T data)
        {
            return new Result<T>
            {
                IsSuccess = true,
                Data = data
            };
        }
/// <summary>
/// Ejecuta la operaciÃ³n public.
/// </summary>

        public static Result<T> Failed(string message)
        {
            return new Result<T>
            {
                IsSuccess = false,
                Reason = message
            };
        }
    }
}

using System;
using static MUFG_T24.Comunes.Comunes;

namespace Aplicacion.Modelos
{
/// <summary>
/// Representa Special International Dto.
/// </summary>

    public class SpecialInternationalDto
    {
        public string UniqueFolio { get; set; }
        public string OperationType { get; set; } = "FXE";
        public string OperationFolio { get; set; }
        public string CustomerCdl { get; set; }
        public string DebitAccount { get; set; }
        public string OperationStatus { get; set; } = "PEND";
        public string CapturingUser { get; set; }
        public string AuthorizingUser { get; set; }
        public DateTime CapturingDate { get; set; }
        public DateTime AuthorizingDate { get; set; }
        public double DebitAmount { get; set; }
        public string CustomerShortName { get; set; }
        public string DebitCurrency { get; set; }
        public string RelatedForex { get; set; }
        public double SettledAmount { get; set; }
        public string DebitTheirRef { get; set; }
        public string AccountType { get; set; }
        public string PaymentType { get; set; }
        public string ChargeType { get; set; }
        public int ValueDate { get; set; }
        public string BeneficiaryName { get; set; }
        public string Comment { get; set; }
        public string BeneficiaryTaxId { get; set; }
        public string OrderingCustomerTaxId { get; set; }
        public string RetentionRate { get; set; }
        public string BeneficiaryBankName { get; set; }
        public string BeneficiaryBankBic { get; set; }
        public string BeneficiaryAccount { get; set; }
        public string CorrespondingBankName { get; set; }
        public string CorrespondingBankBic { get; set; }
        public string CorrespondingBankAccount { get; set; }
        public string Message { get; set; }
        public string FormatCountry { get; set; }
        public string TemplateId { get; set; }
        public string FormatTemplateId { get; set; }
        public string BeneficiaryCountry { get; set; }
        public string BeneficiaryCity { get; set; }
        public string BeneficiaryStreet { get; set; }
        public string BeneficiarySuburb { get; set; }
        public string BeneficiaryBuildingNumber { get; set; }
        public string BeneficiaryZipCode { get; set; }

        /// <summary>
        /// Ejecuta la operaciÃ³n public.
        /// </summary>
        public static SpecialInternationalDto MapFromValoresFormato(
            object[] valoresFormato)
        {
            return new SpecialInternationalDto
            {
                OperationType = Convert.ToString(valoresFormato[29]),
                OperationFolio = Convert.ToString(valoresFormato[30]),
                CustomerCdl = Convert.ToString(valoresFormato[31]),
                DebitAccount = Convert.ToString(valoresFormato[32]),
                CapturingUser = Convert.ToString(valoresFormato[34]),
                AuthorizingUser = Convert.ToString(valoresFormato[36]),
                CapturingDate = Convert.ToDateTime(valoresFormato[33]),
                AuthorizingDate = Convert.ToDateTime(valoresFormato[35]),
                DebitAmount = Convert.ToDouble(valoresFormato[37]),
                CustomerShortName = TextoBESS(valoresFormato[1]),
                DebitCurrency = Convert.ToString(valoresFormato[3]),
                RelatedForex = Convert.ToString(valoresFormato[6]),
                SettledAmount = Convert.ToDouble(valoresFormato[8]),
                DebitTheirRef = TextoBESS(valoresFormato[9]),
                AccountType = TextoBESS(valoresFormato[10]),
                PaymentType = TextoBESS(valoresFormato[11]),
                ChargeType = TextoBESS(valoresFormato[12]),
                ValueDate = Convert.ToInt32(valoresFormato[13]),
                BeneficiaryName = TextoBESS(valoresFormato[14]),
                Comment = TextoBESS(valoresFormato[15]),
                BeneficiaryTaxId = TextoBESS(valoresFormato[16]),
                RetentionRate = Convert.ToString(valoresFormato[17]),
                BeneficiaryBankName = TextoBESS(valoresFormato[18]),
                BeneficiaryBankBic = Convert.ToString(valoresFormato[21]),
                CorrespondingBankName = TextoBESS(valoresFormato[23]),
                CorrespondingBankBic = Convert.ToString(valoresFormato[24]),
                BeneficiaryAccount = TextoBESS(valoresFormato[25]),
                CorrespondingBankAccount = TextoBESS(valoresFormato[25]), // Contrato legacy FXE: ambos campos comparten origen.
                Message = TextoBESS(valoresFormato[27]),
                FormatCountry = Convert.ToString(valoresFormato[28]),
                TemplateId = Convert.ToString(valoresFormato[38]),
                FormatTemplateId = Convert.ToString(valoresFormato[39]),
                BeneficiaryCountry = Convert.ToString(valoresFormato[40]),
                BeneficiaryCity = Convert.ToString(valoresFormato[41]),
                BeneficiaryStreet = Convert.ToString(valoresFormato[42]),
                BeneficiarySuburb = Convert.ToString(valoresFormato[43]),
                BeneficiaryBuildingNumber = Convert.ToString(valoresFormato[44]),
                BeneficiaryZipCode = Convert.ToString(valoresFormato[45])
            };
        }

    }

}

using Aplicacion.DTOs.CNT;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.CNT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ICNTDao
    {
        /// <summary>
        /// Actualiza datos del plan de trabajo maestro a partir de la información de la concentradora asociada al folio solicitado.
        /// </summary>
        Task<bool> ActualizarDatosPlanTrabajoMaestroDesdeConcentradora(string folio, string tipoOperacion, string estatus, string folioUnico, int fechaValor, string monedaDefaultMn);
        /// <summary>
        /// Actualiza el folio único de la concentradora asociada al folio solicitado.
        /// </summary>
        Task<bool> ActualizarFolioUnicoConcentradora(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string referencia);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalidaSrv(string referencia, int numCliente, double cdl, string ordenante, double operacion, string operacionDesc, string tipo, string tipoDesc, double cuenta, string nombre, string cuentaMoneda, double via, string viaDesc, decimal importe, decimal saldo, string comentarios, DateTime fechaCaptura, DateTime fechaValor, string hora, string capturista, double fad, string referenciaControl, double referenciaControlCont);
        /// <summary>
        /// Obtiene los datos de la concentradora asociados al folio solicitado.
        /// </summary>
        Task<CntConcentradoraDto> ObtenerConcentradoraPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerEntradaSalidaPorReferencia.
        /// </summary>
        Task<CntEntradaSalidaDto> ObtenerEntradaSalidaPorReferencia(string referencia);
    }
}

using Aplicacion.DTOs.ComunSPID;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IComunSPIDDao
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarFirma.
        /// </summary>
        Task<bool> ActualizarFirma(string tabla, string folio, string firma);
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerDatosFirma.
        /// </summary>
        Task<SpidFirmaDigitalDto> ObtenerDatosFirma(string tabla, string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerDatosGeneracionFirma.
        /// </summary>
        Task<SpidGeneraFirmaDto> ObtenerDatosGeneracionFirma(string tabla, string folio);
    }
}

using Aplicacion.DTOs.ENT_COAS;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.ENT_COAS
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IENTCOASSpeiDao
    {
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<List<EntCoasParticipanteSpeiDto>> ObtenerParticipantesSpei();
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntCoasRespuestaSpeiDto> ResponderEntradaSpei(EntCoasResponderEntradaSpeiRequestDto dto);
    }
}

using Aplicacion.DTOs.ENT_COAS;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.ENT_COAS
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IENTCOASTomiDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ActualizarPlanTrabajoMaestroDatosGenerales definida para ENT_COAS.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroDatosGenerales(string folio, string tipoOperacion, string folioUnico, string fechaValor, string moneda, decimal importe, string subtipoOperacion, int? numCliente, string nombreCliente, string numCuenta);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ActualizarPlanTrabajoMaestroRecibidaNoAbonada definida para ENT_COAS.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroRecibidaNoAbonada(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_CancelarPlanTrabajoMaestroDevuelto definida para ENT_COAS.
        /// </summary>
        Task<bool> CancelarPlanTrabajoMaestroDevuelto(string folio, string tipoOperacion, string razonRechazo, string fechaValor, string moneda, decimal importe);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ContarEntradasSalidasPorFolioUnico definida para ENT_COAS.
        /// </summary>
        Task<int> ContarEntradasSalidasPorFolioUnico(string folioUnico);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ContarEntradasSpeiPorFolio definida para ENT_COAS.
        /// </summary>
        Task<int> ContarEntradasSpeiPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_FinalizarDetallePlanTrabajoExceptoCep definida para ENT_COAS.
        /// </summary>
        Task<bool> FinalizarDetallePlanTrabajoExceptoCep(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_FinalizarPlanTrabajoMaestroRecibidaNoAbonada definida para ENT_SPID.
        /// </summary>
        Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonada(string folio, string tipoOperacion, string subtipoOperacion, string fechaValor, string moneda, decimal importe, string descripcionError);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBbc definida para ENT_COAS.
        /// </summary>
        Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBbc(string folio, string tipoOperacion, string subtipoOperacion, string fechaValor, string moneda, decimal importe, string descripcionError);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_InsertarDevolucionCoasDesdeOrdenRecibida definida para ENT_COAS.
        /// </summary>
        Task<bool> InsertarDevolucionCoasDesdeOrdenRecibida(string folio, int fechaSistema, int causaDevolucion);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(EntCoasInsertarEntradaSalidaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoSpei(EntCoasInsertarFormatoSpeiDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarEntradaSpeiConfirmadaPendiente definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarEntradaSpeiConfirmadaPendiente(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarEntradaSpeiProcesada definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarEntradaSpeiProcesada(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarEntradaSpeiRechazada definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarEntradaSpeiRechazada(string folio, int causaDevolucion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarOrdenEmitidaComoDevuelta definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarOrdenEmitidaComoDevuelta(int fechaOper, int insClave, decimal importe, string cveRastreo, int causaDevolucion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarOrdenRecibidaAceptada definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarOrdenRecibidaAceptada(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarOrdenRecibidaDevueltaPorFolio definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarOrdenRecibidaDevueltaPorFolio(string folio, int causaDevolucion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_MarcarOrdenRecibidaRechazada definida para ENT_COAS.
        /// </summary>
        Task<bool> MarcarOrdenRecibidaRechazada(string folio, int causaDevolucion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerConciliacionEntradaSalidaPorReferencia.
        /// </summary>
        Task<EntCoasConciliacionEntradaSalidaDto> ObtenerConciliacionEntradaSalidaPorReferencia(string folioUnico);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ObtenerConciliacionTomiPorFolio definida para ENT_SPEI.
        /// </summary>
        Task<EntCoasConciliacionTomiDto> ObtenerConciliacionTomiPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ObtenerEntradaSpeiTomiPorFolio definida para ENT_COAS.
        /// </summary>
        Task<EntCoasEntradaSpeiTomiDto> ObtenerEntradaSpeiTomiPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ObtenerOrdenEmitidaDevueltaParaValidacion definida para ENT_COAS.
        /// </summary>
        Task<EntCoasOrdenPagoDto> ObtenerOrdenEmitidaDevueltaParaValidacion(int fechaOper, int insClave, decimal importe, string cveRastreo);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTCOAS_ObtenerOrdenPagoRecibidaPorFolio definida para ENT_COAS.
        /// </summary>
        Task<EntCoasOrdenPagoDto> ObtenerOrdenPagoRecibidaPorFolio(string folio, int fechaSistema);
    }
}
using Aplicacion.DTOs.ENT_SPEI;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPEI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IEntSpeiSpeiDao
    {
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntSpeiCambioEstatusRespuestaDto> CambiarEstatusRecibida(decimal opClave, decimal opFechaOper, decimal opFolio, string opCveRastreo, decimal opMonto, decimal? cdClave, string opEstatus);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> EjecutarDevolucion(string folio, int causaDevolucion, string firmaDigital);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntSpeiDataDevolucionDto> ObtenerDataDevolucionPorFolio(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntSpeiOperacionRecibidaDto[]> ObtenerOperacionesRecibidasPorFecha(decimal fechaOperacion);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntSpeiParticipanteDto[]> ObtenerParticipantes();
    }
}
using Aplicacion.DTOs.ENT_SPEI;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPEI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IEntSpeiDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ActualizarPlanDevolucion definida para ENT_SPEI.
        /// </summary>
        Task<bool> ActualizarPlanDevolucion(string folio, string tipoOperacion, string razonRechazo, int fechaEjecucion, string moneda, decimal importe);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ActualizarPlanTrabajoMaestroDatosGral definida para ENT_SPEI.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroDatosGral(string folio, string tipoOperacion, string folioUnico, int fechaValor, string moneda, decimal importe, string subtipoOperacion, bool incluirDatosCliente, string numCliente, string nombreCliente, string numCuenta);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ActualizarPlanTrabajoRechazoSinAbono definida para ENT_SPEI.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoRechazoSinAbono(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ActualizarPlanTrabajoRecibidaNoAbonada definida para ENT_SPEI.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoRecibidaNoAbonada(string folio, string tipoOperacion, string subtipoOperacion, string descripcionError, int fechaValor, string moneda, decimal importe, bool flgTerminado);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_CerrarPlanTrabajoDetalleExceptoCep definida para ENT_SPEI.
        /// </summary>
        Task<bool> CerrarPlanTrabajoDetalleExceptoCep(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ContarNfEntradasSpeiPorFolio definida para ENT_SPEI.
        /// </summary>
        Task<int> ContarNfEntradasSpeiPorFolio(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalidaSrv(string referencia, int numCliente, double cdl, string ordenante, double operacion, string operacionDesc, string tipo, string tipoDesc, double cuenta, string nombre, string cuentaMoneda, double via, string viaDesc, decimal importe, decimal saldo, string comentarios, DateTime fechaCaptura, DateTime fechaValor, string hora, string capturista, double fad, string referenciaControl, double referenciaControlCont);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoSpei(long opClave, int opFechaOper, int meClave, int opFolio, long insClave, decimal opMonto, int tpClave, string opCveRastreo, string opNomOrd, int tcClaveOrd, string opCuentaOrd, string opRfcCurpOrd, string opNomBen, int? tcClaveBen, string opCuentaBen, string opRfcCurpBen, string opNomBen2, int? tcClaveBen2, string opCuentaBen2, string opRfcCurpBen2, string opConceptoPago, string opConceptoPag2, decimal? opIva, int? opRefNumerica, string opRefCobranza, string opClavePago, int? toClave, int? cdClave, string opEstado, string opCde, int? opFechaCap, int? opHoraCap, string opTipoOrden, int? opPrioridad, string usuClave, long? opFolioServidor, string opRazonRechazo, string opUsuAutoriza, int? opHoraLiqBm, long? opHoraLiqSist, string nfEstatus, string nfFolioUnico, string t24CustomerCode, string t24CompanyName, string t24BenAcctNo, string nfBancoBenef, int nfFechaEjecucion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_MarcarNfEntradaSpeiComoConp definida para ENT_SPEI.
        /// </summary>
        Task<bool> MarcarNfEntradaSpeiComoConp(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_MarcarNfEntradaSpeiComoProcesada definida para ENT_SPEI.
        /// </summary>
        Task<bool> MarcarNfEntradaSpeiComoProcesada(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ObtenerCatalogoDevolucionesAutomaticas definida para ENT_SPEI.
        /// </summary>
        Task<EntSpeiAutoDevolucionDto[]> ObtenerCatalogoDevolucionesAutomaticas();
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerConciliacionEntradaSalidaPorReferencia.
        /// </summary>
        Task<EntSpeiEntradaSalidaConciliacionDto> ObtenerConciliacionEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ObtenerConciliacionTomiPorFolio definida para ENT_SPEI.
        /// </summary>
        Task<EntSpeiTomiConciliacionDto> ObtenerConciliacionTomiPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ObtenerCuentaVostro definida para ENT_SPEI.
        /// </summary>
        Task<string> ObtenerCuentaVostro(string claveSwift, string cuentaBeneficiario);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPEI_ObtenerNfEntradaSpeiPorFolio definida para ENT_SPEI.
        /// </summary>
        Task<EntSpeiNfEntradaSpeiDto> ObtenerNfEntradaSpeiPorFolio(string folio);
    }
}
using Aplicacion.DTOs.ENT_SPID;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.ENT_SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IENTSPIDSpidDao
    {
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> CambiarEstatusRecibidaSpid(EntSpidCambiarEstatusRecibidaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> DevolverEntradaSpid(EntSpidDevolverEntradaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntSpidDatosDevolucionDto> ObtenerDatosDevolucionSpid(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<List<EntSpidOperacionRecibidaDto>> ObtenerOperacionesRecibidasPorFechaOperacion(decimal fechaOperacion);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<List<EntSpidParticipanteDto>> ObtenerParticipantesSpid();
    }
}
using Aplicacion.DTOs.ENT_SPID;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.ENT_SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IENTSPIDTomiDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ActualizarDatosPlanTrabajoMaestroLectura definida para FXD.
        /// </summary>
        Task<bool> ActualizarDatosPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, decimal fechaValor, string moneda, decimal importe, string subtipoOperacion, bool actualizarDatosCliente, int? numCliente, string nombreCliente, string numCuenta);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_CancelarPlanTrabajoMaestroPorDevolucion definida para ENT_SPID.
        /// </summary>
        Task<bool> CancelarPlanTrabajoMaestroPorDevolucion(string folio, string tipoOperacion, string razonRechazo, decimal fechaValor, string moneda, decimal importe);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_ContarEntradasSpidPorFolio definida para ENT_SPID.
        /// </summary>
        Task<int> ContarEntradasSpidPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_FinalizarPlanTrabajoMaestroRecibidaNoAbonada definida para ENT_SPID.
        /// </summary>
        Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonada(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBTMUM definida para ENT_SPID.
        /// </summary>
        Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBTMUM(string folio, string tipoOperacion, string subtipoOperacion, decimal fechaValor, string moneda, decimal importe, string descripcionError);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(EntSpidInsertarEntradaSalidaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoSpidEnTomi(EntSpidInsertarFormatoTomiDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_MarcarEntradaSpidConfirmadaPendiente definida para ENT_SPID.
        /// </summary>
        Task<bool> MarcarEntradaSpidConfirmadaPendiente(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_MarcarEntradaSpidProcesada definida para ENT_SPID.
        /// </summary>
        Task<bool> MarcarEntradaSpidProcesada(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerConciliacionEntradaSalidaPorReferencia.
        /// </summary>
        Task<EntSpidConciliacionEntradaSalidaDto> ObtenerConciliacionEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_ObtenerConciliacionEntradaSpidPorFolio definida para ENT_SPID.
        /// </summary>
        Task<EntSpidConciliacionTomiDto> ObtenerConciliacionEntradaSpidPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTSPID_ObtenerEntradaSpidParaEntradaSalida definida para ENT_SPID.
        /// </summary>
        Task<EntSpidEntradaSpidParaEntradaSalidaDto> ObtenerEntradaSpidParaEntradaSalida(string folio);
    }
}
using Aplicacion.DTOs.ENT_TEF;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.ENT_TEF
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IENTTEFTefDao
    {
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntTefCambiarEstatusResultadoDto> CambiarEstatusRecibida(long idMaster, int fechaProceso, string razonRechazo, string estatus);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<List<EntTefParticipanteDto>> ObtenerParticipantes();
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<EntTefRecibidaDto> ObtenerRecibidaPorFolio(int fechaAplicacion, long idOperacion);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<List<EntTefRecibidaDto>> ObtenerRecibidasPorFechaAplicacion(int fechaAplicacion);
    }
}
using Aplicacion.DTOs.ENT_TEF;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.ENT_TEF
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IENTTEFTomiDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ActualizarDatosGeneralesMaestroConCliente definida para ENT_TEF.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesMaestroConCliente(long folio, string tipoOperacion, string folioUnico, int fechaValor, string moneda, decimal importe, string subtipoOperacion, int numCliente, string nombreCliente, string numCuenta);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ActualizarDatosGeneralesMaestroSinCliente definida para ENT_TEF.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesMaestroSinCliente(long folio, string tipoOperacion, string folioUnico, int fechaValor, string moneda, decimal importe, string subtipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ActualizarEstatusFormatoEntradaTefAScnp definida para ENT_TEF.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoEntradaTefAScnp(long idOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_CancelarMaestroPorDevolucion definida para ENT_TEF.
        /// </summary>
        Task<bool> CancelarMaestroPorDevolucion(long folio, string tipoOperacion, string razonRechazo, int fechaValor, string moneda, decimal importe);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ExisteEntradaSalidaPorReferencia.
        /// </summary>
        Task<bool> ExisteEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ExisteFormatoEntradaTef definida para ENT_TEF.
        /// </summary>
        Task<bool> ExisteFormatoEntradaTef(long idOperacion);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(EntTefEntradaSalidaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoEntradaTef(EntTefInsertarFormatoTomiDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_MarcarMaestroRecibidaNoAbonada definida para ENT_TEF.
        /// </summary>
        Task<bool> MarcarMaestroRecibidaNoAbonada(long folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ObtenerConciliacionEntradaSalida definida para ENT_TEF.
        /// </summary>
        Task<EntTefConciliacionEntradaSalidaDto> ObtenerConciliacionEntradaSalida(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ObtenerConciliacionFormatoEntradaTef definida para ENT_TEF.
        /// </summary>
        Task<EntTefConciliacionTomiDto> ObtenerConciliacionFormatoEntradaTef(long idOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaENTTEF_ObtenerFormatoEntradaTef definida para ENT_TEF.
        /// </summary>
        Task<EntTefFormatoTomiDto> ObtenerFormatoEntradaTef(long idOperacion);
    }
}
using Aplicacion.DTOs.FXD;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXD
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXDCmsDao
    {
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXDActualizarFormatoCmsAceptadoHoy definida para FXD.
        /// </summary>
        Task<bool> ActualizarFormatoCmsAceptadoHoy(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaSERActualizarFormatoCmsPendienteEjecucion.
        /// </summary>
        Task<bool> ActualizarFormatoCmsPendienteEjecucion(string folio);
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXDActualizarFormatoCmsRechazadoPorForexCancelado definida para FXD.
        /// </summary>
        Task<bool> ActualizarFormatoCmsRechazadoPorForexCancelado(string folio);
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXDObtenerFormatoFxdCmsPorFolio definida para FXD.
        /// </summary>
        Task<FormatoFxdCmsDto> ObtenerFormatoFxdCmsPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXD;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXD
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXDDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ActualizarDatosPlanTrabajoMaestroLectura definida para FXD.
        /// </summary>
        Task<bool> ActualizarDatosPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string estatus, string folioUnico, int fechaValor, string moneda, decimal importe, string numCliente, string nombreCliente, string numCuenta, string subtipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ActualizarPlanTrabajoMaestroCanceladoPorForex definida para FXD.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroCanceladoPorForex(string folio, string tipoOperacion, int fechaValor, string moneda, decimal importe, string descripcionError);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ActualizarTombinFxdConfirmadoHoy definida para FXD.
        /// </summary>
        Task<bool> ActualizarTombinFxdConfirmadoHoy(string folio, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ActualizarTombinFxdPendienteFechaFutura definida para FXD.
        /// </summary>
        Task<bool> ActualizarTombinFxdPendienteFechaFutura(string folio, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ActualizarTombinFxdRechazadoPorForexCancelado definida para FXD.
        /// </summary>
        Task<bool> ActualizarTombinFxdRechazadoPorForexCancelado(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ContarRegistrosTombinFxdPorFolio definida para FXD.
        /// </summary>
        Task<int> ContarRegistrosTombinFxdPorFolio(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalidaSrv(string referencia, int numCliente, double cdl, string ordenante, double operacion, string operacionDesc, string tipo, string tipoDesc, double cuenta, string nombre, string cuentaMoneda, double via, string viaDesc, double importe, double saldo, string comentarios, DateTime fechaCaptura, DateTime fechaValor, string hora, string capturista, double fad, string referenciaControl, double referenciaControlCont);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoFxdEnTomi(string nfFolioUnico, string nfTipo, long nfFolio, string t24CustomerCode, string t24DebitAcctNo, string nfEstatus, string nfUsuarioCaptura, string nfUsuarioAutoriza, DateTime nfFechaCaptura, DateTime? nfFechaAutoriza, decimal t24DebitAmount, string t24ShortName, string nfForexRelacionado, string t24DebitCurrency, string nfBancoBenef, string t24BenAcctNo, string nfTipoCuenta, string t24BenCustName, string nfRfcBeneficiario, string nfTasaRetencion, int nfFechaEjecucion, string t24DebitTheirRef, decimal nfMontoEnviar, string idPlantilla, long? idFormatoPlantilla, decimal? opRefNumerica);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerEntradaSalidaPorReferencia.
        /// </summary>
        Task<FxdEntradaSalidaDto> ObtenerEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerForexStatusPorExternalRef.
        /// </summary>
        Task<ForexStatusDto> ObtenerForexStatusPorExternalRef(string externalRef);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ObtenerTombinFxdParaConciliacion definida para FXD.
        /// </summary>
        Task<TombinFxdConciliacionDto> ObtenerTombinFxdParaConciliacion(string folio, string estatusCsv);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXD_ObtenerTombinFxdPorFolio definida para FXD.
        /// </summary>
        Task<TombinFxdEntradaSalidaDto> ObtenerTombinFxdPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXD;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXD
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXDGPHDao
    {
        /// <summary>
        /// Actualiza el folio único de FXD GPH asociado al folio solicitado.
        /// </summary>
        Task<bool> ActualizarFolioUnicoFxdGph(string folio, string folioUnico);
        /// <summary>
        /// Actualiza el plan de trabajo maestro con datos de FXD GPH asociados al folio solicitado.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroDesdeFxdGph(string folio, string tipoOperacion, string folioUnico, int fechaValor, string monedaDefault);
        /// <summary>
        /// Obtiene los datos de FXD GPH asociados al folio solicitado.
        /// </summary>
        Task<FxdGphDatosDto> ObtenerDatosFxdGphPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXD;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXD
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXDH2HDao
    {
        /// <summary>
        /// Obtiene los datos de FXD H2H asociados al folio solicitado.
        /// </summary>
        Task<FxdH2hDatosDto> ObtenerDatosFxdH2hPorFolio(string folio);
        /// <summary>
        /// Actualiza el plan de trabajo maestro con datos de FXD H2H asociados al folio solicitado.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroDesdeFxdH2h(
            string folio,
            string tipoOperacion,
            string folioUnico,
            int fechaValor,
            string monedaDefault);
    }
}
using Aplicacion.DTOs.FXD_Local;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXDLocal
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXDLocalT24Dao
    {
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXDLocalEsBancoPorCustomerCode definida para FXD_Local.
        /// </summary>
        Task<bool> EsBancoPorCustomerCode(string customerCode);
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXDLocalObtenerCommissionTypeT24 definida para FXD_Local.
        /// </summary>
        Task<FxdLocalCommissionTypeT24Dto> ObtenerCommissionTypeT24(string commissionType);
    }
}
using Aplicacion.DTOs.FXD_Local;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXDLocal
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXDLocalDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXDLocal_ActualizarDatosComision definida para FXD_Local.
        /// </summary>
        Task<bool> ActualizarDatosComision(long folio, decimal montoSpeiEnvio, string commissionType, string commissionCode, string chargeCode);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn, string fechaEjecucion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXDLocal_EsPacs008PorFolio definida para FXD_Local.
        /// </summary>
        Task<bool> EsPacs008PorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxdLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXDLocal_ObtenerDatosComisionPorFolio definida para FXD_Local.
        /// </summary>
        Task<FxdLocalDatosComisionDto> ObtenerDatosComisionPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXI;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFxiCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTEFActualizarEstatusFormatoCms.
        /// </summary>
        Task<bool> ActualizarEstatusFormato(FxiActualizarEstatusFormatoAceptadoCmsDto dto);
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXIActualizarEstatusFormatoRechazadoForex definida para FXI.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoRechazadoForex(long idFormato);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTMDObtenerFormatoPorFolio.
        /// </summary>
        Task<FxiFormatoCmsDto> ObtenerFormatoPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXIObtenerRfcOrdenante definida para FXI.
        /// </summary>
        Task<FxiClienteRfcCmsDto> ObtenerRfcOrdenante(decimal clienteT24);
    }
}
using Aplicacion.DTOs.FXI;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFxiT24Dao
    {
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXIObtenerRfcOrdenante definida para FXI.
        /// </summary>
        Task<FxiClienteRfcCmsDto> ObtenerRfcOrdenante(decimal clienteT24);
    }
}
using Aplicacion.DTOs.FXI;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFxiTomiDao
    {
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ActualizarEstatusFormatoFxi definida para FXI.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoFxi(FxiActualizarEstatusNfFxiTomiDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ActualizarFormatoFxiRechazadoForex definida para FXI.
        /// </summary>
        Task<bool> ActualizarFormatoFxiRechazadoForex(long folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ActualizarMaestroDatosGenerales definida para FXI.
        /// </summary>
        Task<bool> ActualizarMaestroDatosGenerales(FxiActualizarMaestroDatosGeneralesTomiDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ActualizarMaestroFechaFutura definida para FXI.
        /// </summary>
        Task<bool> ActualizarMaestroFechaFutura(long folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_CancelarMaestroForex definida para FXI.
        /// </summary>
        Task<bool> CancelarMaestroForex(FxiCancelarMaestroForexTomiDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ExisteEntradaSalida definida para FXI.
        /// </summary>
        Task<bool> ExisteEntradaSalida(string folioUnico);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ExisteFormatoFxi definida para FXI.
        /// </summary>
        Task<bool> ExisteFormatoFxi(long folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(FxiInsertarEntradaSalidaTomiDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoFxe(FxiInsertarFormatoFxiTomiDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoFxi(FxiInsertarFormatoFxiTomiDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarRegistroSpeiCobertura(long folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ObtenerDatosConciliacionEntradaSalida definida para FXI.
        /// </summary>
        Task<FxiEntradaSalidaConciliacionTomiDto> ObtenerDatosConciliacionEntradaSalida(string folioUnico);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ObtenerDatosConciliacionFormatoFxi definida para FXI.
        /// </summary>
        Task<FxiConciliacionNfFxiTomiDto> ObtenerDatosConciliacionFormatoFxi(long folio, bool forexCancelado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerEstatusForex.
        /// </summary>
        Task<FxiForexEstatusTomiDto> ObtenerEstatusForex(string externalRef);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ObtenerFormatoFxi definida para FXI.
        /// </summary>
        Task<FxiTombinNfFxiTomiDto> ObtenerFormatoFxi(long folio);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ObtenerNostro definida para FXI.
        /// </summary>
        Task<FxiNostroCuentaTomiDto> ObtenerNostro(string nostroAccountIdentifier, string nostroAccountCurrency);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXI_ObtenerFormatoFxi definida para FXI.
        /// </summary>
        Task<List<FxiTombinNfFxiTomiDto>> ObtenerFormatosFxi(long folio);
    }
}
using Aplicacion.DTOs.FXIGPH;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXIGPH
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXI_GPHDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaFXTGph_InsertarDetalleOperacionGph.
        /// </summary>
        Task<bool> InsertarDetalleOperacionGph(string archivo, long folioArchivo, string idOperacion, string tipoOperacion, string estatus, string error);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaFXTH2H_MarcarFechaFutura.
        /// </summary>
        Task<bool> MarcarFechaFutura(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxiGphDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXI_H2H;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXIGPH
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXI_H2HDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaFXTH2H_MarcarFechaFutura.
        /// </summary>
        Task<bool> MarcarFechaFutura(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxiH2HDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXI_Local;
using System.Threading.Tasks;
using static Aplicacion.DTOs.FXI_Local.FXI_LocalDto;

namespace Aplicacion.Interfaces.FXILocal
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXI_LocalDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxiLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXT;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFxtCmsDao
    {
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXTActualizarEstatusFormatoRechazado definida para FXT.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoRechazado(FxtActualizarEstatusFormatoRechazadoDto dto);
        /// <summary>
        /// Ejecuta la operación MUFG_MX2_LecturaFXTActualizarEstatusFormatoSimple definida para FXT.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoSimple(FxtActualizarEstatusFormatoSimpleDto dto);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTMDObtenerFormatoPorFolio.
        /// </summary>
        Task<FxtFormatoCmsDto> ObtenerFormatoPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXT;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFxtTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ActualizarDatosGeneralesPlanTrabajo.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesPlanTrabajo(FxtActualizarDatosGeneralesPlanTrabajoDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXT_ActualizarEstatusFxtConfirmacion definida para FXT.
        /// </summary>
        Task<bool> ActualizarEstatusFxtConfirmacion(FxtActualizarEstatusFxtConfirmacionDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXT_ActualizarEstatusFxtRechazadoForex definida para FXT.
        /// </summary>
        Task<bool> ActualizarEstatusFxtRechazadoForex(FxtActualizarEstatusFxtRechazadoForexDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_CancelarPlanTrabajoPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoPorForex(FxtCancelarPlanTrabajoPorForexDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXT_ExisteFormatoFxtPorFolio definida para FXT.
        /// </summary>
        Task<bool> ExisteFormatoFxtPorFolio(long folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoFxt(FxtInsertarFormatoFxtTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_MarcarPlanTrabajoFechaFutura.
        /// </summary>
        Task<bool> MarcarPlanTrabajoFechaFutura(FxtMarcarFechaFuturaPlanTrabajoDto dto);
        /// <summary>
        /// Ejecuta la operación usp_LecturaFXT_ObtenerConciliacionFxtPorFolio definida para FXT.
        /// </summary>
        Task<FxtConciliacionTomiDto> ObtenerConciliacionFxtPorFolio(long folio, bool forexCancelado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerStatusForexPorExternalRef.
        /// </summary>
        Task<FxtForexStatusTomiDto> ObtenerStatusForexPorExternalRef(string externalRef);
    }
}
using Aplicacion.DTOs.FXT_GPH;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXT_GPH
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXT_GPHDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxtGphDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaFXTH2H_MarcarFechaFutura.
        /// </summary>
        Task<bool> MarcarFechaFutura(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaFXTGph_InsertarDetalleOperacionGph.
        /// </summary>
        Task<bool> InsertarDetalleOperacionGph(string archivo, long folioArchivo, string idOperacion, string tipoOperacion, string estatus, string error);
    }
}
using Aplicacion.DTOs.FXT_H2H;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXTH2H
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXTH2HDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaFXTH2H_MarcarFechaFutura.
        /// </summary>
        Task<bool> MarcarFechaFutura(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxtH2HDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
    }
}
using Aplicacion.DTOs.FXT_Local;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXT_Local
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IFXT_LocalDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<FxtLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
    }
}
using Aplicacion.Modelos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FirmaElectronica
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IComunFXD
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n GeneraFirmaDigital.
        /// </summary>
        Task <bool> GeneraFirmaDigital(string strTabla, string strFolio, string strFolioT24, bool? strFlgNomina = false, int? intTipoPago = 1, string strReferencia_emisor = "");
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtieneCampoFirma.
        /// </summary>
        Task<string> ObtieneCampoFirma(OrdenPagoValue OrdenPagoValue);
        /// <summary>
        /// Ejecuta la operaciÃ³n ValidaSPEIEntrada.
        /// </summary>
        bool ValidaSPEIEntrada(OrdenPagoValue OrdenPagoValue, string dataSigned);
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FirmaElectronica
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IComunSPID
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n GeneraFirmaDigitalSPID.
        /// </summary>
        Task<bool> GeneraFirmaDigitalSPID(string strTabla, string strFolio, string strFolioT24, bool? strFlgNomina, int? intTipoPago, string strReferencia_emisor = "");
        /// <summary>
        /// Ejecuta la operaciÃ³n ValidaFirmaDigitalSPID.
        /// </summary>
        Task<bool> ValidaFirmaDigitalSPID(string strTabla, string strFolio, string strFolioT24, bool? strFlgNomina, int? intTipoPago, string strReferencia_emisor = "");
    }
}
using Aplicacion.DTOs.MFX;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.MFX
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IMFXCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaMFXAceptarFormatoForex.
        /// </summary>
        Task<bool> AceptarFormatoForex(object folioCms);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaMFXCancelarAutorizacionForex.
        /// </summary>
        Task<bool> CancelarAutorizacionForex(long idAutorizacion);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaMFXInsertarAutorizacionForexDesdeTomi.
        /// </summary>
        Task<bool> InsertarAutorizacionForexDesdeTomi(long idAutorizacion, long idCliente, string divisaEntrega, string divisaAdquiere, decimal montoEntrega, decimal montoAdquiere, double ecRate, string fechaValor, string cuentaDebito, string cuentaCredito, string tipo);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaMFXObtenerFormatosForexConfirmados.
        /// </summary>
        Task<List<MFXFormatoCmsDto>> ObtenerFormatosForexConfirmados(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaMFXRechazarFormatoForex.
        /// </summary>
        Task<bool> RechazarFormatoForex(object folioCms);
    }
}
using Aplicacion.DTOs.MFX;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.MFX
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IMFXVistasT24Dao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaMFXObtenerCuentaCustomer.
        /// </summary>
        Task<MFXCuentaCustomerT24Dto> ObtenerCuentaCustomer(string numeroCuenta);
    }
}
using Aplicacion.DTOs.MFX;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.MFX
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IMFXTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ActualizarMaestroDatosGral.
        /// </summary>
        Task<bool> ActualizarMaestroDatosGral(string folio, string tipoOperacion, string fechaValorTexto, string monedaEntrega, decimal importeEntrega, long numeroCliente, string nombreCliente, string cuentaEntrega);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ActualizarSolicitudForexValidacionSistema.
        /// </summary>
        Task<bool> ActualizarSolicitudForexValidacionSistema(long folioForexNormalizado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ActualizarTombinMfxConfirmado.
        /// </summary>
        Task<bool> ActualizarTombinMfxConfirmado(long folioCms, string fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ActualizarTombinMfxRechazado.
        /// </summary>
        Task<bool> ActualizarTombinMfxRechazado(long folioCms);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_CancelarMaestroPorForexCanceladoBTMUMDesdeEsperandoConfirmacion.
        /// </summary>
        Task<bool> CancelarMaestroPorForexCanceladoBTMUMDesdeEsperandoConfirmacion(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_CancelarMaestroPorForexCanceladoBTMUMDesdeLectura.
        /// </summary>
        Task<bool> CancelarMaestroPorForexCanceladoBTMUMDesdeLectura(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_CancelarMaestroPorForexCanceladoBTMUMDesdeTesoreria.
        /// </summary>
        Task<bool> CancelarMaestroPorForexCanceladoBTMUMDesdeTesoreria(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_CancelarMaestroPorForexCanceladoCMS.
        /// </summary>
        Task<bool> CancelarMaestroPorForexCanceladoCMS(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ContarTombinMfxPorFolio.
        /// </summary>
        Task<int> ContarTombinMfxPorFolio(long folioCms);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoMfxDesdeCms(MFXFormatoTomiInsertDto datos);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_MarcarMaestroEnvioCms.
        /// </summary>
        Task<bool> MarcarMaestroEnvioCms(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_MarcarMaestroEsperandoAutorizacionTesoreria.
        /// </summary>
        Task<bool> MarcarMaestroEsperandoAutorizacionTesoreria(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_MarcarMaestroEsperandoConfirmacion.
        /// </summary>
        Task<bool> MarcarMaestroEsperandoConfirmacion(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_MarcarMaestroLecturaConFolioCms.
        /// </summary>
        Task<bool> MarcarMaestroLecturaConFolioCms(string folio, string tipoOperacion, long folioCms);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_MarcarSolicitudForexProcesada.
        /// </summary>
        Task<bool> MarcarSolicitudForexProcesada(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ObtenerDatosForexParaConciliacion.
        /// </summary>
        Task<MFXForexConciliacionDto> ObtenerDatosForexParaConciliacion(long folioForexNormalizado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ObtenerDatosTomiParaConciliacion.
        /// </summary>
        Task<MFXTomiConciliacionDto> ObtenerDatosTomiParaConciliacion(long folioCms, bool forexCancelado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMFX_ObtenerSolicitudForexPorFolio.
        /// </summary>
        Task<MFXSolicitudForexTomiDto> ObtenerSolicitudForexPorFolio(string folio);
    }
}
using Aplicacion.DTOs.MercadoDinero;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.MercadoDinero
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IMercadoDineroTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ActualizarDatosComplementariosEnTablaTrabajo.
        /// </summary>
        Task<bool> ActualizarDatosComplementariosEnTablaTrabajo(string tablaTrabajo, long folio, int? tipoPagoSpei, string tipoCuenta, string bancoBenef, string cuentaBeneficiario, string nombreBeneficiario);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ActualizarEstatusCalypsoConp.
        /// </summary>
        Task<bool> ActualizarEstatusCalypsoConp(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ActualizarEstatusCalypsoEnProceso.
        /// </summary>
        Task<bool> ActualizarEstatusCalypsoEnProceso(long folio, string estatus);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ActualizarPlanTrabajoMaestroYFolioUnico.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroYFolioUnico(long folio, string tipoOperacion, string folioUnico, int fechaValor, string moneda, decimal importe, string numCliente, string nombreCliente, string numCuenta, string subtipoOperacion, string desAgregado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_InsertarSwiftOut.
        /// </summary>
        Task<bool> InsertarSwiftOut(string referenciaSwf, int fechaEjecucion, string moneda, decimal importe, string transactionRef, string datoCalypso, string mensajeSwift);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_MarcarPlanTrabajoEsperandoAutorizacionUsuario.
        /// </summary>
        Task<bool> MarcarPlanTrabajoEsperandoAutorizacionUsuario(string tipoOperacion, long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_MarcarPlanTrabajoValidandoInfo.
        /// </summary>
        Task<bool> MarcarPlanTrabajoValidandoInfo(string tipoOperacion, long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ObtenerComplementoPagoPorTipoLiquidacionYCdl.
        /// </summary>
        Task<MercadoDineroComplementoPagoDto> ObtenerComplementoPagoPorTipoLiquidacionYCdl(string tipoLiquidacion, string cdl);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ObtenerDuplicidadesLiquidacionPorFolio.
        /// </summary>
        Task<List<MercadoDineroDuplicidadLiquidacionDto>> ObtenerDuplicidadesLiquidacionPorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ObtenerEstatusParPorFolioSalida.
        /// </summary>
        Task<MercadoDineroEstatusParDto> ObtenerEstatusParPorFolioSalida(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaMD_ObtenerLiquidacionPorFolio.
        /// </summary>
        Task<MercadoDineroLiquidacionTomiDto> ObtenerLiquidacionPorFolio(long folio);
    }
}
using Aplicacion.DTOs.NOM_FXD;
using System.Threading.Tasks;

/// <summary>
/// Interfaz
/// </summary>
public interface INOMFXDCmsDao
{
    /// <summary>
    /// Ejecuta el procedimiento MUFG_MX2_LecturaNOMFXDMarcarNominaAceptada.
    /// </summary>
    Task<bool> MarcarNominaAceptada(string idFormatoNomina, string mensaje, string estatus);
    /// <summary>
    /// Ejecuta el procedimiento MUFG_MX2_LecturaNOMFXDMarcarNominaRechazadaPorForexCancelado.
    /// </summary>
    Task<bool> MarcarNominaRechazadaPorForexCancelado(string idFormatoNomina, string mensaje, string estatus, string razonRechazo);
    /// <summary>
    /// Ejecuta el procedimiento MUFG_MX2_LecturaNOMFXDMarcarNominaRecibidaPendienteEjecucion.
    /// </summary>
    Task<bool> MarcarNominaRecibidaPendienteEjecucion(string idFormatoNomina, string mensaje, string estatus);
    /// <summary>
    /// Ejecuta el procedimiento MUFG_MX2_LecturaNOMFXDObtenerDetalleNominaPorFolio.
    /// </summary>
    Task<NomFxdDetalleCmsDto> ObtenerDetalleNominaPorFolio(string folio);
    /// <summary>
    /// Ejecuta el procedimiento MUFG_MX2_LecturaNOMFXDObtenerMaestroNominaPorFolio.
    /// </summary>
    Task<NomFxdMaestroCmsDto> ObtenerMaestroNominaPorFolio(string folio);
}
using Aplicacion.DTOs.NOM_FXD;
using System.Threading.Tasks;

/// <summary>
/// Interfaz
/// </summary>
public interface INOMFXDTomiDao
{
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
    /// </summary>
    Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string folioMaestro, string tipoOperacion, string folioUnico, string fechaValor, string moneda, decimal importeDetalle, decimal importeMaestro, string numCliente, string nombreCliente, string numCuenta, string subtipoOperacion, int numPagos);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_CancelarPlanTrabajoPorForexCanceladoDetalle.
    /// </summary>
    Task<bool> CancelarPlanTrabajoPorForexCanceladoDetalle(string folio, string tipoOperacion, string fechaValor, string moneda, decimal importe, string errorDescripcion);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_CancelarPlanTrabajoPorForexCanceladoMaestro.
    /// </summary>
    Task<bool> CancelarPlanTrabajoPorForexCanceladoMaestro(string folioMaestro, string tipoOperacion, string fechaValor, string moneda, decimal importe, string errorDescripcion);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_ContarDetalleNominaPorFolio.
    /// </summary>
    Task<int> ContarDetalleNominaPorFolio(string folio);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_ContarDetalleNominaProcesadoPorFolio.
    /// </summary>
    Task<int> ContarDetalleNominaProcesadoPorFolio(string folio);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaTAX_ContarEntradaSalidaPorReferencia.
    /// </summary>
    Task<int> ContarEntradaSalidaPorReferencia(string referencia);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_ContarMaestroNominaPorFolio.
    /// </summary>
    Task<int> ContarMaestroNominaPorFolio(string folioMaestro);
    /// <summary>
    /// Procedimiento legacy.
    /// </summary>
    Task<bool> InsertarEntradaSalidaMaestro(NomFxdEntradaSalidaMaestroDto dto);
    /// <summary>
    /// Procedimiento legacy.
    /// </summary>
    Task<bool> InsertarFormatoFxdNominaDetalle(NomFxdInsertarDetalleTomiDto dto);
    /// <summary>
    /// Procedimiento legacy.
    /// </summary>
    Task<bool> InsertarFormatoFxdNominaMaestro(NomFxdInsertarMaestroTomiDto dto);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_MarcarNominaTomiAceptadaHoy.
    /// </summary>
    Task<bool> MarcarNominaTomiAceptadaHoy(string idFormatoNomina, string nfFolio, string usuarioConfirma, string fechaConfirma);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_MarcarNominaTomiFechaFutura.
    /// </summary>
    Task<bool> MarcarNominaTomiFechaFutura(string idFormatoNomina, string nfFolio, string usuarioConfirma, System.DateTime fechaConfirma);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_MarcarNominaTomiRechazadaPorForexCancelado.
    /// </summary>
    Task<bool> MarcarNominaTomiRechazadaPorForexCancelado(string idFormatoNomina, string nfFolio, string estatus, string razon);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_MarcarProcesosDetalleTerminados.
    /// </summary>
    Task<bool> MarcarProcesosDetalleTerminados(string folioMaestro, string tipoOperacion, string servicio, string aliasProceso, int pid, string processName);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_ObtenerDetalleConciliacionPorFolio.
    /// </summary>
    Task<NomFxdConciliacionDetalleTomiDto> ObtenerDetalleConciliacionPorFolio(string folio);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerEntradaSalidaPorReferencia.
    /// </summary>
    Task<NomFxdEntradaSalidaConciliacionDto> ObtenerEntradaSalidaPorReferencia(string referencia);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaNOMFXD_ObtenerNominaMaestroParaEntradaSalida.
    /// </summary>
    Task<NomFxdNominaMaestroParaEntradaSalidaDto> ObtenerNominaMaestroParaEntradaSalida(string folioMaestro);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerStatusForexPorExternalRef.
    /// </summary>
    Task<NomFxdForexStatusDto> ObtenerStatusForexPorExternalRef(string externalRef);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerSumarizadoDetallePorMaestro.
    /// </summary>
    Task<NomFxdSumarizadoDetalleTomiDto> ObtenerSumarizadoDetallePorMaestro(string idFormatoNomina);
    /// <summary>
    /// Ejecuta el procedimiento usp_LecturaPaPTEF_ObtenerSumarizadoDetalleProcesadoPorMaestro.
    /// </summary>
    Task<NomFxdSumarizadoDetalleTomiDto> ObtenerSumarizadoDetalleProcesadoPorMaestro(string idFormatoNomina);
}
using Aplicacion.DTOs.PaP;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPFXDCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFActualizarCmsAceptadoHoy.
        /// </summary>
        Task<bool> ActualizarCmsAceptadoHoy(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFActualizarCmsPendienteEjecucion.
        /// </summary>
        Task<bool> ActualizarCmsPendienteEjecucion(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFActualizarCmsRechazadoPorForexCancelado.
        /// </summary>
        Task<bool> ActualizarCmsRechazadoPorForexCancelado(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXDObtenerDetallePorFolio.
        /// </summary>
        Task<PaPFxdDetalleCmsDto> ObtenerDetallePapFxdPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXDObtenerMaestroPorDetalle.
        /// </summary>
        Task<PaPFxdMaestroCmsDto> ObtenerMaestroPapFxdPorDetalle(string folioDetalle);
    }
}
using Aplicacion.DTOs.PaP;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPTEFCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFActualizarCmsAceptadoHoy.
        /// </summary>
        Task<bool> ActualizarCmsAceptadoHoy(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFActualizarCmsPendienteEjecucion.
        /// </summary>
        Task<bool> ActualizarCmsPendienteEjecucion(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFActualizarCmsRechazadoPorForexCancelado.
        /// </summary>
        Task<bool> ActualizarCmsRechazadoPorForexCancelado(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFObtenerDetallePorFolio.
        /// </summary>
        Task<PaPTefDetalleCmsDto> ObtenerDetallePapTefPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPTEFObtenerMaestroPorDetalle.
        /// </summary>
        Task<PaPTefMaestroCmsDto> ObtenerMaestroPapTefPorDetalle(string folioDetalle);
    }
}
using Aplicacion.DTOs.PaP;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPTEFDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ActualizarDatosGeneralesPlanTrabajo.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesPlanTrabajo(PaPTEFActualizarDatosGeneralesPlanTrabajoDto datos);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ActualizarTomiAceptadoHoy.
        /// </summary>
        Task<bool> ActualizarTomiAceptadoHoy(string folioMaestro, string fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ActualizarTomiFechaFutura.
        /// </summary>
        Task<bool> ActualizarTomiFechaFutura(string folioMaestro, string fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ActualizarTomiRechazadoPorForexCancelado.
        /// </summary>
        Task<bool> ActualizarTomiRechazadoPorForexCancelado(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_CancelarPlanTrabajoDetallePorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoDetallePorForex(PaPTEFCancelarPlanTrabajoForexDto datos);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_CancelarPlanTrabajoMaestroPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoMaestroPorForex(PaPTEFCancelarPlanTrabajoMaestroForexDto datos);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ContarDetallePapTomiPorFolio.
        /// </summary>
        Task<int> ContarDetallePapTomiPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ContarDetallePapTomiProcesadoPorFolio.
        /// </summary>
        Task<int> ContarDetallePapTomiProcesadoPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ContarEntradaSalidaPorReferencia.
        /// </summary>
        Task<int> ContarEntradaSalidaPorReferencia(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ContarMaestroPapTomiPorFolio.
        /// </summary>
        Task<int> ContarMaestroPapTomiPorFolio(string folioMaestro);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoTefPapDetalle(PaPTefInsertarDetalleTomiDto datos);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoTefPapMaestro(PaPTefInsertarMaestroTomiDto datos);
        /// <summary>
        /// Ejecuta la operaciÃ³n InsertarFormatoTefPapMaestroExitoso.
        /// </summary>
        Task<bool> InsertarFormatoTefPapMaestroExitoso(PaPTefInsertarMaestroTomiDto datos);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarSrvEntradasSalidas(PaPTefInsertarEntradaSalidaTomiDto datos);
        /// <summary>
        /// Ejecuta la operaciÃ³n InsertarSrvEntradasSalidasExitoso.
        /// </summary>
        Task<bool> InsertarSrvEntradasSalidasExitoso(PaPTefInsertarEntradaSalidaTomiDto datos);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerDetalleTomiParaConciliacion.
        /// </summary>
        Task<PaPTEFDetalleTomiConciliacionDto> ObtenerDetalleTomiParaConciliacion(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerDetalleTomiParaEntradaSalida.
        /// </summary>
        Task<PaPTefDetalleTomiEntradaSalidaDto> ObtenerDetalleTomiParaEntradaSalida(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ObtenerEntradaSalidaParaConciliacion.
        /// </summary>
        Task<PaPTEFEntradaSalidaConciliacionDto> ObtenerEntradaSalidaParaConciliacion(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerEstatusForex.
        /// </summary>
        Task<PaPTEFForexStatusDto> ObtenerEstatusForex(string externalRef);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerSumarizadoDetallePorMaestro.
        /// </summary>
        Task<PaPTEFSumarizadoDetalleTomiDto> ObtenerSumarizadoDetallePorMaestro(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ObtenerSumarizadoDetalleProcesadoPorMaestro.
        /// </summary>
        Task<PaPTEFSumarizadoDetalleTomiDto> ObtenerSumarizadoDetalleProcesadoPorMaestro(string folioMaestro);
    }
}
using Aplicacion.DTOs.PaP;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPFXDDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ActualizarPlanTrabajoDetalleCanceladoPorForex.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroDetalleCanceladoPorForex(long folioDetalle, string tipoOperacion, int fechaValor, string moneda, decimal importe, string descripcionError);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ActualizarPlanTrabajoMaestroCanceladoPorForex.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroMaestroCanceladoPorForex(long folioMaestro, string tipoOperacion, int fechaValor, string moneda, decimal importe, string descripcionError);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroPapFxdLectura(long folioDetalle, string tipoOperacion, string folioUnico, int fechaValor, string moneda, decimal importeDetalle, decimal importeMaestro, string numCliente, string nombreCliente, string numCuenta, string subtipoOperacion, int noPagos, long folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ActualizarTomiAceptadoHoy.
        /// </summary>
        Task<bool> ActualizarTomiAceptadoHoy(long folioMaestro, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ActualizarTomiPendienteEjecucion.
        /// </summary>
        Task<bool> ActualizarTomiPendienteEjecucion(long folioMaestro, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPTEF_ActualizarTomiRechazadoPorForexCancelado.
        /// </summary>
        Task<bool> ActualizarTomiRechazadoPorForexCancelado(long folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ContarDetallesConEstatusDistintoPend.
        /// </summary>
        Task<int> ContarDetallesPapFxdConEstatusDistintoPend(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ContarRegistrosDetallePorFolio.
        /// </summary>
        Task<int> ContarRegistrosPapFxdDetallePorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ContarRegistrosMaestroPorFolio.
        /// </summary>
        Task<int> ContarRegistrosPapFxdMaestroPorFolio(long folioMaestro);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalidaSrv(string referencia, int numCliente, double cdl, string ordenante, double operacion, string operacionDesc, string tipo, string tipoDesc, double cuenta, string nombre, string cuentaMoneda, double via, string viaDesc, double importe, double saldo, string comentarios, DateTime fechaCaptura, DateTime fechaValor, string hora, string capturista, double fad, string referenciaControl, double referenciaControlCont);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoPapFxdDetalleEnTomi(string nfFolioUnico, string nfTipo, long nfFolio, string t24CustomerCode, string t24DebitAcctNo, string nfEstatus, string nfUsuarioCaptura, string nfUsuarioAutoriza, DateTime nfFechaCaptura, DateTime? nfFechaAutoriza, decimal t24DebitAmount, string t24ShortName, string nfForexRelacionado, string t24DebitCurrency, string nfBancoBenef, string t24BenAcctNo, string nfTipoCuenta, string t24BenCustName, string nfRfcBeneficiario, string nfTasaRetencion, int nfFechaEjecucion, string t24DebitTheirRef, decimal nfMontoEnviar, string idPlantilla, long? idFormatoPlantilla, long? idFilesBtmum, int? noPagos, string pagosDescripcion, int? noPagosProcesados, int? noPagosDevueltos, long? idFormatoPap, string ref1, string ref2, string ref3, string ref4, string ref5);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoPapFxdMaestroEnTomi(string nfFolioUnico, string nfTipo, long nfFolio, string t24CustomerCode, string t24DebitAcctNo, string nfEstatus, string nfUsuarioCaptura, string nfUsuarioAutoriza, DateTime nfFechaCaptura, DateTime? nfFechaAutoriza, decimal t24DebitAmount, string t24ShortName, string nfForexRelacionado, string t24DebitCurrency, string nfBancoBenef, string t24BenAcctNo, string nfTipoCuenta, string t24BenCustName, string nfRfcBeneficiario, string nfTasaRetencion, int nfFechaEjecucion, string t24DebitTheirRef, decimal nfMontoEnviar, string idPlantilla, long? idFormatoPlantilla, long? idFilesBtmum, int? noPagos, string pagosDescripcion, int? noPagosProcesados, int? noPagosDevueltos);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerDetalleTomiPorFolio.
        /// </summary>
        Task<List<PapFxdDetalleTomiDto>> ObtenerDetallePapFxdTomiPorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerDetalleProcesadoPorFolio.
        /// </summary>
        Task<PapFxdDetalleImporteDto> ObtenerDetalleProcesadoPorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerEntradaSalidaPorReferencia.
        /// </summary>
        Task<FxdEntradaSalidaDto> ObtenerEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerForexStatusPorExternalRef.
        /// </summary>
        Task<ForexStatusDto> ObtenerForexStatusPorExternalRef(string externalRef);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXD_ObtenerSumatoriaDetalle.
        /// </summary>
        Task<PapFxdSumatoriaDetalleDto> ObtenerSumatoriaDetallePapFxd(long folioMaestro, bool soloNoPendientes);
    }
}
using Aplicacion.DTOs.PaP_FXI;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_FXI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPFXICmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXIMarcarAceptadoHoyCms.
        /// </summary>
        Task<bool> MarcarAceptadoHoyCms(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXIMarcarFechaFuturaCms.
        /// </summary>
        Task<bool> MarcarFechaFuturaCms(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXIMarcarRechazoForexCanceladoCms.
        /// </summary>
        Task<bool> MarcarRechazoForexCanceladoCms(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXIObtenerDetalleCmsPorFolio.
        /// </summary>
        Task<PaPFxiDetalleCmsDto> ObtenerDetalleCmsPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXIObtenerMaestroCmsPorFolioDetalle.
        /// </summary>
        Task<PaPFxiMaestroCmsDto> ObtenerMaestroCmsPorFolioDetalle(string folio);
    }
}
using Aplicacion.DTOs.PaP_FXI;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_FXI
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPFXITomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ActualizarPlanTrabajoDatosGral.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoDatosGral(string folio, string folioMaestro, string tipoOperacion, string folioUnico, string fechaValor, string moneda, decimal importeDetalle, decimal importeMaestro, string numCliente, string nombreCliente, string numCuenta, int numPagos, string subtipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_CancelarPlanTrabajoDetallePorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoDetallePorForex(string folio, string tipoOperacion, string fechaValor, string moneda, decimal importeDetalle);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_CancelarPlanTrabajoMaestroPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoMaestroPorForex(string folioMaestro, string tipoOperacion, string fechaValor, string moneda, decimal importeMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ContarDetallePapTomiNoPendientePorFolio.
        /// </summary>
        Task<int> ContarDetallePapTomiNoPendientePorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ContarDetallePapTomiPorFolio.
        /// </summary>
        Task<int> ContarDetallePapTomiPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ContarMaestroPapTomiPorFolio.
        /// </summary>
        Task<int> ContarMaestroPapTomiPorFolio(string folioMaestro);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarDetallePapTomi(PaPFxiInsertarDetallePapTomiDto detalle);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(PaPFxiInsertarEntradaSalidaDto entradaSalida);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarMaestroPapTomi(PaPFxiInsertarMaestroPapTomiDto maestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_MarcarAceptadoHoyTomi.
        /// </summary>
        Task<bool> MarcarAceptadoHoyTomi(string folioMaestro, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_MarcarFechaFuturaTomi.
        /// </summary>
        Task<bool> MarcarFechaFuturaTomi(string folioMaestro, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_MarcarPlanTrabajoDetalleFechaFutura.
        /// </summary>
        Task<bool> MarcarPlanTrabajoDetalleFechaFutura(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_MarcarPlanTrabajoMaestroFechaFutura.
        /// </summary>
        Task<bool> MarcarPlanTrabajoMaestroFechaFutura(string folioMaestro, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_MarcarRechazoForexCanceladoTomi.
        /// </summary>
        Task<bool> MarcarRechazoForexCanceladoTomi(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerDetalleTomiParaConciliacion.
        /// </summary>
        Task<PaPFxiDetalleTomiConciliacionDto> ObtenerDetalleTomiParaConciliacion(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerDetalleTomiParaEntradaSalida.
        /// </summary>
        Task<List<PaPFxiDetalleTomiEntradaSalidaDto>> ObtenerDetalleTomiParaEntradaSalida(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ObtenerEntradaSalidaParaConciliacion.
        /// </summary>
        Task<PaPFxiEntradaSalidaConciliacionDto> ObtenerEntradaSalidaParaConciliacion(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerStatusForexPorExternalRef.
        /// </summary>
        Task<PaPFxiForexStatusDto> ObtenerStatusForexPorExternalRef(string folioForex);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerSumarizadoDetalleNoPendientePorMaestro.
        /// </summary>
        Task<PaPFxiSumarizadoDetalleTomiDto> ObtenerSumarizadoDetalleNoPendientePorMaestro(string folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXI_ObtenerSumarizadoDetallePorMaestro.
        /// </summary>
        Task<PaPFxiSumarizadoDetalleTomiDto> ObtenerSumarizadoDetallePorMaestro(string folioMaestro);
    }
}
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_FXT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPFXTCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXTActualizarEstatusFormatoPap.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoPap(PaPFXTActualizarEstatusCmsDto dto);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXTActualizarEstatusFormatoPapSinRazon.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoPapSinRazon(PaPFXTActualizarEstatusCmsDto dto);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXTObtenerFormatoPapDetallePorFolio.
        /// </summary>
        Task<PaPFXTFormatoPapDetalleCmsDto> ObtenerFormatoPapDetallePorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPFXTObtenerFormatoPapMaestroPorFolio.
        /// </summary>
        Task<PaPFXTFormatoPapMaestroCmsDto> ObtenerFormatoPapMaestroPorFolio(string folio);
    }
}
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_FXT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPFXTTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ActualizarDatosGeneralesPlanTrabajo.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesPlanTrabajo(PaPFXTActualizarDatosGeneralesPlanTrabajoTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ActualizarEstatusFormatoPapTomi.
        /// </summary>
        Task<bool> ActualizarEstatusFormatoPapTomi(PaPFXTActualizarEstatusTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_CancelarPlanTrabajoPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoPorForex(PaPFXTCancelarPlanTrabajoPorForexTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ExisteFormatoPapDetallePorFolio.
        /// </summary>
        Task<PaPFXTExisteRegistroTomiDto> ExisteFormatoPapDetallePorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ExisteFormatoPapDetalleProcesadoPorFolio.
        /// </summary>
        Task<PaPFXTExisteRegistroTomiDto> ExisteFormatoPapDetalleProcesadoPorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ExisteFormatoPapMaestroPorFolio.
        /// </summary>
        Task<PaPFXTExisteRegistroTomiDto> ExisteFormatoPapMaestroPorFolio(long folioMaestro);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoPapDetalle(PaPFXTInsertarDetalleTomiDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoPapMaestro(PaPFXTInsertarMaestroTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_MarcarPlanTrabajoFechaFutura.
        /// </summary>
        Task<bool> MarcarPlanTrabajoFechaFutura(PaPFXTMarcarFechaFuturaTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ObtenerEstatusForexPorReferencia.
        /// </summary>
        Task<PaPFXTForexEstatusTomiDto> ObtenerEstatusForexPorReferencia(string folioForex);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ObtenerImporteDetalleConciliacionPorFolio.
        /// </summary>
        Task<PaPFXTConciliacionDetalleTomiDto> ObtenerImporteDetalleConciliacionPorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ObtenerSumarizadoDetallePorIdFormatoPap.
        /// </summary>
        Task<PaPFXTSumarizadoDetalleTomiDto> ObtenerSumarizadoDetallePorIdFormatoPap(long idFormatoPap);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_ObtenerSumarizadoDetalleProcesadoPorIdFormatoPap.
        /// </summary>
        Task<PaPFXTSumarizadoDetalleTomiDto> ObtenerSumarizadoDetalleProcesadoPorIdFormatoPap(long idFormatoPap);
    }
}
using Aplicacion.DTOs.PaP_SPID;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPSpidCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPSPIDActualizarEstatusCmsAceptado.
        /// </summary>
        Task<bool> ActualizarEstatusAceptado(long idFormatoPap);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPSPIDActualizarEstatusCmsPendienteEjecucion.
        /// </summary>
        Task<bool> ActualizarEstatusPendienteEjecucion(long idFormatoPap);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPSPIDActualizarEstatusCmsRechazadoForex.
        /// </summary>
        Task<bool> ActualizarEstatusRechazadoForex(long idFormatoPap);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPSPIDObtenerDetallePorFolio.
        /// </summary>
        Task<PaPSpidFormatoPapDetalleCmsDto> ObtenerDetallePorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaPaPSPIDObtenerMaestroPorFolioDetalle.
        /// </summary>
        Task<PaPSpidFormatoPapCmsDto> ObtenerMaestroPorFolioDetalle(string folio);
    }
}
using Aplicacion.DTOs.PaP_SPID;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPSpidInsightDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_ListCUSTOMEREdoCtaT24.
        /// </summary>
        Task<PaPSpidInsightCustomerEdoCtaDto> ObtenerCustomerEdoCta(string customerCode);
    }
}
using Aplicacion.DTOs.PaP_SPID;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.PaP_SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IPaPSpidTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ActualizarDatosGeneralesPlanTrabajo.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesPlanTrabajo(PaPSpidActualizarPlanTrabajoDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ActualizarEstatusAceptadoHoy.
        /// </summary>
        Task<bool> ActualizarEstatusAceptadoHoy(long folioMaestro, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ActualizarEstatusFechaFutura.
        /// </summary>
        Task<bool> ActualizarEstatusFechaFutura(long folioMaestro, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ActualizarEstatusTomiRechazadoForex.
        /// </summary>
        Task<bool> ActualizarEstatusTomiRechazadoForex(long folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ActualizarPlanTrabajoFechaFutura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoFechaFutura(long folio, long folioMaestro, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_CancelarPlanTrabajoPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoPorForex(long folio, long folioMaestro, string tipoOperacion, int fechaValor, string moneda, decimal importeDetalle, decimal importeMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarDetalleNoPendientePorFolio.
        /// </summary>
        Task<int> ContarDetalleNoPendientePorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarDetallePorFolio.
        /// </summary>
        Task<int> ContarDetallePorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarEntradasSalidasPorReferencia.
        /// </summary>
        Task<int> ContarEntradasSalidasPorReferencia(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ContarMaestroPorFolio.
        /// </summary>
        Task<int> ContarMaestroPorFolio(long folioMaestro);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(PaPSpidInsertarEntradaSalidaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoDetalle(PaPSpidInsertarFormatoDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoMaestro(PaPSpidInsertarFormatoDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerDetalleConciliacion.
        /// </summary>
        Task<PaPSpidDetalleConciliacionDto> ObtenerDetalleConciliacion(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerDetallesParaEntradaSalida.
        /// </summary>
        Task<List<PaPSpidTombinDetalleDto>> ObtenerDetallesParaEntradaSalida(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerEntradaSalidaConciliacion.
        /// </summary>
        Task<PaPSpidEntradaSalidaConciliacionDto> ObtenerEntradaSalidaConciliacion(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerEstatusForex.
        /// </summary>
        Task<PaPSpidForexStatusDto> ObtenerEstatusForex(string externalRef);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerSumarizadoDetalle.
        /// </summary>
        Task<PaPSpidSumarizadoDetalleDto> ObtenerSumarizadoDetalle(long folioMaestro);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaP_SPID_ObtenerSumarizadoDetalleNoPendiente.
        /// </summary>
        Task<PaPSpidSumarizadoDetalleDto> ObtenerSumarizadoDetalleNoPendiente(long folioMaestro);
    }
}
using Aplicacion.DTOs.SAT;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SAT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISATCmsDao
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerFormatoSatPorFolio.
        /// </summary>
        Task<SAT_DTO> ObtenerFormatoSatPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerFormatoSatDetallePorFolio.
        /// </summary>
        Task<SAT_Formato_DTO> ObtenerFormatoSatDetallePorFolio(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarFormatoRechazado.
        /// </summary>
        Task<bool> ActualizarFormatoRechazado(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarFormatoAceptadoConSello.
        /// </summary>
        Task<bool> ActualizarFormatoAceptadoConSello(string folio, string cadenaOriginal, string selloDigital, string formatoXmlDet);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarFormatoPendienteDebito.
        /// </summary>
        Task<bool> ActualizarFormatoPendienteDebito(string folio, string estatus, string formatoXmlDet);
    }
}
using Aplicacion.DTOs.SAT;
using Aplicacion.Modelos.SAT;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SAT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISATLegacyProcessor
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ProcesarCmsATomi.
        /// </summary>
        Task<SAT_DatosRegresoModelo> ProcesarCmsATomi(string folio, int idFormato, string folioUnico);
    }
}
using Aplicacion.DTOs.SAT;
using Aplicacion.Modelos.SAT;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SAT
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISATTomiDao
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerNombreClientePorCdl.
        /// </summary>
        Task<SAT_HdClientes_DTO> ObtenerNombreClientePorCdl(string cdl);
        /// <summary>
        /// Ejecuta la operaciÃ³n ProcesarCmsATomi.
        /// </summary>
        Task<SAT_DatosRegresoModelo> ProcesarCmsATomi(string folio, int idFormato, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string subtipo, string fechaValor, string moneda, decimal importe, string numCliente, string nombreCliente, string numCuenta);
        /// <summary>
        /// Ejecuta la operaciÃ³n ContarTombinCmsSatPorFolio.
        /// </summary>
        Task<int> ContarTombinCmsSatPorFolio(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarTombinFormatoRechazado.
        /// </summary>
        Task<bool> ActualizarTombinFormatoRechazado(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarTombinFormatoProcesado.
        /// </summary>
        Task<bool> ActualizarTombinFormatoProcesado(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarTombinFormatoConp.
        /// </summary>
        Task<bool> ActualizarTombinFormatoConp(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarTombinFormatoConf.
        /// </summary>
        Task<bool> ActualizarTombinFormatoConf(string folio);
        /// <summary>
        /// Ejecuta la operaciÃ³n CancelarPlanTrabajoInvalida.
        /// </summary>
        Task<bool> CancelarPlanTrabajoInvalida(string folio, string tipoOperacion, string fechaValor, string moneda, decimal importe);
        /// <summary>
        /// Ejecuta la operaciÃ³n MarcarPlanTrabajoProcesadaYDetalleTerminado.
        /// </summary>
        Task<bool> MarcarPlanTrabajoProcesadaYDetalleTerminado(string folio, string tipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ObtenerDatosConciliacion.
        /// </summary>
        Task<SAT_Tombin_CMS_Modelo> ObtenerDatosConciliacion(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaPaPFXT_MarcarPlanTrabajoFechaFutura.
        /// </summary>
        Task<bool> MarcarPlanTrabajoFechaFutura(string folio, string tipoOperacion, string fechaValor);
    }
}
using Aplicacion.DTOs.SER;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SER
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISERCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaSERObtenerFormatoCmsPorFolio.
        /// </summary>
        Task<SerFormatoCmsDto> ObtenerFormatoCmsPorFolio(string folio);
/// <summary>
/// Ejecuta el procedimiento MUFG_MX2_LecturaSERActualizarFormatoCmsAceptado.
/// </summary>

        Task<bool> ActualizarFormatoCmsAceptado(string folio);
/// <summary>
/// Ejecuta el procedimiento MUFG_MX2_LecturaSERActualizarFormatoCmsPendienteEjecucion.
/// </summary>

        Task<bool> ActualizarFormatoCmsPendienteEjecucion(string folio);
    }

}
using Aplicacion.DTOs.SER;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SER
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISERTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(SerActualizarPlanTrabajoMaestroDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSER_ActualizarSerPayAceptado.
        /// </summary>
        Task<bool> ActualizarSerPayAceptado(string folio, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSER_ActualizarSerPayFechaFutura.
        /// </summary>
        Task<bool> ActualizarSerPayFechaFutura(string folio, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ContarEntradaSalidaPorReferencia.
        /// </summary>
        Task<int> ContarEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSER_ContarSerPayPorFolio.
        /// </summary>
        Task<int> ContarSerPayPorFolio(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(SerInsertarEntradaSalidaDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarSerPayDesdeCms(SerInsertarSerPayDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSER_ObtenerEntradaSalidaConciliacionPorReferencia.
        /// </summary>
        Task<SerEntradaSalidaConciliacionDto> ObtenerEntradaSalidaConciliacionPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSER_ObtenerSerPayConciliacionPorFolio.
        /// </summary>
        Task<SerPayConciliacionDto> ObtenerSerPayConciliacionPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSER_ObtenerSerPayParaEntradaSalida.
        /// </summary>
        Task<SerPayEntradaSalidaDto> ObtenerSerPayParaEntradaSalida(string folio);
    }
}
using Aplicacion.DTOs.SPID;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISPIDCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTMDObtenerFormatoPorFolio.
        /// </summary>
        Task<SPID_Formato_DTO> ObtenerFormatoPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaSPIDActualizarFormato.
        /// </summary>
        Task<bool> ActualizarFormato(string folio, string mensaje, string estatus, string razonRechazo);
    }
}
using Aplicacion.DTOs.SPID;
using System.Threading.Tasks;
using static Aplicacion.DTOs.SPID.SPIDLocalDtos;

namespace Aplicacion.Interfaces.SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISPIDLocalDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<SpidLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(long folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(long folio, string tipoOperacion, string folioUnico, int fechaEjecucion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPIDGph_MarcarErrorCuentaInvalida.
        /// </summary>
        Task<bool> MarcarErrorCuentaInvalida(long folio, string tipoOperacion, string errorDescripcion);
    }
}
using Aplicacion.DTOs.SPID;
using Aplicacion.DTOs.SPID_GPH;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISpidVistasT24Dao
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerDireccionSpidPorCdl.
        /// </summary>
        Task<SPID_InsightDireccion_DTO> ObtenerDireccionSpidPorCdl(string cdl, string bsDate);
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerDireccionSpidGphPorCdl.
        /// </summary>
        Task<SpidGphInsightDireccionDto> ObtenerDireccionSpidGphPorCdl(string cdl, string bsDate);
    }
}
using Aplicacion.DTOs.SPID;
using Aplicacion.Modelos.SPID;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISPIDTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerStatusForexPorExternalRef.
        /// </summary>
        Task<SPID_ForexBancosFax_DTO> ObtenerStatusForexPorExternalRef(string externalRef);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string fechaEjecucion, string moneda, decimal importe, string numCliente, string nombreCliente, string numCuenta, string subTipo);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoDesdeCms(SPID_InsertarFormatoModelo modelo);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ContarTombinNfPorFolio.
        /// </summary>
        Task<int> ContarTombinNfPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_ObtenerTombinNfPorFolio.
        /// </summary>
        Task<SPID_Tombin_NF_DTO> ObtenerTombinNfPorFolio(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(SPID_InsertarEntradasSalidasModelo modelo);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ContarEntradaSalidaPorReferencia.
        /// </summary>
        Task<int> ContarEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_ActualizarTombinNfEstatus.
        /// </summary>
        Task<bool> ActualizarTombinNfEstatus(string folio, string estatus, string razon);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_CancelarPlanTrabajoPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoPorForex(string folio, string tipoOperacion, string fechaEjecucion, string moneda, decimal importe);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_ObtenerConciliacionNf.
        /// </summary>
        Task<SPID_Tombin_NF_DTO> ObtenerConciliacionNf(string folio, bool forexCancelado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_ObtenerConciliacionEs.
        /// </summary>
        Task<SPID_Tombin_ES_DTO> ObtenerConciliacionEs(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPIDGph_ExisteDiaNoHabil.
        /// </summary>
        Task<bool> ExisteDiaNoHabil(DateTime fecha);
    }
}
using Aplicacion.DTOs.SPID_GPH;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISPIDGphDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<SpidGphDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_H2H_ActualizarDireccion.
        /// </summary>
        Task<bool> ActualizarDireccion(string folio, string direccion, string codigoPostal);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPIDGph_ExisteDiaNoHabil.
        /// </summary>
        Task<bool> ExisteDiaNoHabil(DateTime fecha);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPIDGph_MarcarErrorCuentaInvalida.
        /// </summary>
        Task<bool> MarcarErrorCuentaInvalida(string folio, string tipoOperacion, string errorDescripcion);
    }
}
using Aplicacion.DTOs.SPID_H2H;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID_H2H
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISpidH2hTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_H2H_ActualizarDireccion.
        /// </summary>
        Task<bool> ActualizarDireccion(SpidH2hActualizarDireccionDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_H2H_ActualizarPlanTrabajoMaestro.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestro(SpidH2hActualizarPlanTrabajoMaestroDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_H2H_MarcarPlanTrabajoConError.
        /// </summary>
        Task<bool> MarcarPlanTrabajoConError(SpidH2hMarcarPlanTrabajoConErrorDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_H2H_ObtenerDatosOperacion.
        /// </summary>
        Task<SpidH2hDatosOperacionDto> ObtenerDatosOperacion(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaSPID_H2H_ObtenerDiaNoHabil.
        /// </summary>
        Task<SpidH2hDiaNoHabilDto> ObtenerDiaNoHabil(DateTime fecha);
    }
}
using Aplicacion.DTOs.SPID_H2H;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.SPID_H2H
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ISpidH2hVistasT24Dao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_ListCUSTOMEREdoCtaT24.
        /// </summary>
        Task<SpidH2hCustomerEdoCtaDto> ObtenerCustomerEdoCta(string customerCode, DateTime bsDate);
    }
}
using Aplicacion.DTOs.TAX;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TAX
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITAXCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTAXActualizarFormatoAceptado.
        /// </summary>
        Task<bool> ActualizarFormatoAceptado(long folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTAXActualizarFormatoRecibidoPendienteEjecucion.
        /// </summary>
        Task<bool> ActualizarFormatoRecibidoPendienteEjecucion(long folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTAXObtenerFormatoTaxPorFolio.
        /// </summary>
        Task<TaxFormatoCmsDto> ObtenerFormatoTaxPorFolio(long folio);
    }
}
using Aplicacion.DTOs.TAX;
using System;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TAX
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITAXTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ActualizarDatosGeneralesMaestro.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesMaestro(long folio, string folioUnico, decimal fechaValor, string moneda, decimal importe, int? numCliente, string nombreCliente, string numCuenta, string subtipoOperacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ActualizarTaxPayAceptado.
        /// </summary>
        Task<bool> ActualizarTaxPayAceptado(long folio, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ActualizarTaxPayFechaFutura.
        /// </summary>
        Task<bool> ActualizarTaxPayFechaFutura(long folio, string usuarioConfirma, DateTime fechaConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ContarEntradaSalidaPorReferencia.
        /// </summary>
        Task<int> ContarEntradaSalidaPorReferencia(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ContarTaxPayPorFolio.
        /// </summary>
        Task<int> ContarTaxPayPorFolio(long folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalidaTax(string referencia, int numCliente, double cdl, string ordenante, double operacion, string operacionDesc, string tipo, string tipoDesc, double cuenta, string nombre, string cuentaMoneda, double via, string viaDesc, double importe, double saldo, string comentarios, DateTime? fechaCaptura, DateTime? fechaValor, string hora, string capturista, double fad, string referenciaControl, double referenciaControlCont);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoTaxPay(string nfTipo, double nfFolio, string t24CustomerCode, string t24ShortName, string nfEstatus, string nfUsuarioCaptura, string nfUsuarioAutoriza, DateTime? nfFechaCaptura, DateTime? nfFechaAutoriza, int nfFechaEjecucion, string t24BenCustName, string t24NoAgreement, string t24Currency, string t24Reference, double t24Amount, string t24Concept, string t24AcctNo, double nfMontoEnviar, string nfFolioUnico, double? idFormatoPlantilla, string nfTipoCuenta, string idPlantilla);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ObtenerEntradaSalidaParaConciliacion.
        /// </summary>
        Task<TaxEntradaSalidaConciliacionDto> ObtenerEntradaSalidaParaConciliacion(string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ObtenerTaxPayParaConciliacion.
        /// </summary>
        Task<TaxPayConciliacionDto> ObtenerTaxPayParaConciliacion(long folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTAX_ObtenerTaxPayParaEntradaSalida.
        /// </summary>
        Task<TaxPayEntradaSalidaDto> ObtenerTaxPayParaEntradaSalida(long folio);
    }
}
using Aplicacion.DTOs.TEF;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TEF
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITEFCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTEFActualizarEstatusFormatoCms.
        /// </summary>
        Task<bool> ActualizarEstatusFormato(TefActualizarEstatusCmsDto dto);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTMDObtenerFormatoPorFolio.
        /// </summary>
        Task<TefFormatoCmsDto> ObtenerFormatoPorFolio(string folio);
    }
}
using Aplicacion.DTOs.TEF_GPH;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TEF
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITEFGphDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<TefGphDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
    }
}
using Aplicacion.DTOs.TEF_Local;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TEF
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITEFLocalDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ObtenerDatosBasicosPorFolio.
        /// </summary>
        Task<TefLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEFLocal_ActualizarFolioUnico.
        /// </summary>
        Task<bool> ActualizarFolioUnico(string folio, string folioUnico);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string monedaDefaultMn);
    }
}
using Aplicacion.DTOs.TEF;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TEF
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITEFTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ActualizarDatosGeneralesPlanTrabajo.
        /// </summary>
        Task<bool> ActualizarDatosGeneralesPlanTrabajo(TefActualizarDatosGeneralesPlanTrabajoDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ActualizarEstatusTef.
        /// </summary>
        Task<bool> ActualizarEstatusTef(TefActualizarEstatusTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_CancelarPlanTrabajoPorForex.
        /// </summary>
        Task<bool> CancelarPlanTrabajoPorForex(TefCancelarPlanTrabajoPorForexDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ExisteEntradaSalidaPorReferencia.
        /// </summary>
        Task<bool> ExisteEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ExisteFormatoTefPorFolio.
        /// </summary>
        Task<bool> ExisteFormatoTefPorFolio(string folio);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarEntradaSalida(TefInsertarEntradaSalidaTomiDto dto);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoTef(TefInsertarFormatoTomiDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerConciliacionEntradaSalidaPorReferencia.
        /// </summary>
        Task<TefConciliacionEntradaSalidaTomiDto> ObtenerConciliacionEntradaSalidaPorReferencia(string referencia);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerConciliacionTefPorFolio.
        /// </summary>
        Task<TefConciliacionTomiDto> ObtenerConciliacionTefPorFolio(string folio, bool forexCancelado);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerEntradaSalidaOrigenPorFolio.
        /// </summary>
        Task<TefEntradaSalidaOrigenTomiDto> ObtenerEntradaSalidaOrigenPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_ObtenerStatusForexPorExternalRef.
        /// </summary>
        Task<TefForexStatusTomiDto> ObtenerStatusForexPorExternalRef(string externalRef);
    }
}
using Aplicacion.DTOs.TEF_H2H;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TEF_H2H
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITefH2hTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_H2H_ActualizarPlanTrabajoMaestro.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestro(TefH2hActualizarPlanTrabajoMaestroDto dto);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTEF_H2H_ObtenerDatosOperacion.
        /// </summary>
        Task<TefH2hDatosOperacionDto> ObtenerDatosOperacion(long folio);
    }
}
using Aplicacion.DTOs.TMD;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TMD
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITMDCmsDao
    {
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTMDObtenerFormatoPorFolio.
        /// </summary>
        Task<TMD_Formato_DTO> ObtenerFormatoPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento MUFG_MX2_LecturaTMDActualizarFormatoEstado.
        /// </summary>
        Task<bool> ActualizarFormatoEstado(string folio, string mensaje, string estatus);
    }
}
using Aplicacion.DTOs.TMD;
using Aplicacion.Modelos.TMD;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.TMD
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface ITMDTomiDao
    {
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ObtenerSolicitudEspecialPorId.
        /// </summary>
        Task<TMD_Solicitudes_DTO> ObtenerSolicitudEspecialPorId(string idAutorizacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarPlanTrabajoMaestroLectura.
        /// </summary>
        Task<bool> ActualizarPlanTrabajoMaestroLectura(string folio, string tipoOperacion, string folioUnico, string fechaEjecucion, string moneda, decimal importe, string numCliente, string nombreCliente, string numCuenta, string subtipo);
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        Task<bool> InsertarFormatoTmd(TMD_InsertarFormatoModelo modelo);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ContarTombinNfPorFolio.
        /// </summary>
        Task<int> ContarTombinNfPorFolio(string folio);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_MarcarSolicitudEspecialProcesada.
        /// </summary>
        Task<bool> MarcarSolicitudEspecialProcesada(string idAutorizacion);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ActualizarNfEstatus.
        /// </summary>
        Task<bool> ActualizarNfEstatus(string folio, string estatus, string usuarioConfirma);
        /// <summary>
        /// Ejecuta el procedimiento usp_LecturaTMD_ObtenerDatosConciliacion.
        /// </summary>
        Task<TMD_Tombin_Modelo> ObtenerDatosConciliacion(string folio, bool forexCancelado);
    }
}
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IGeneralDao
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ActualizarFallo.
        /// </summary>
        Task<bool> ActualizarFallo(string folio, string tipoOperacion, string servicio, string aliasProceso, string errDescripcion);
    }
}
using Aplicacion.DTOs.ENT_SPID;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces
{
    /// <summary>
    /// Interfaz
    /// </summary>
    public interface IT24CuentaWebService
    {
        /// <summary>
        /// Ejecuta la operaciÃ³n ObtenerInformacionCuenta.
        /// </summary>
        Task<T24InformacionCuentaDto> ObtenerInformacionCuenta(string account);
    }
}
using Aplicacion.Interfaces;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Threading.Tasks;

namespace Infraestructura
{
    /// <summary>
    /// Representa General Dao.
    /// </summary>
    public class GeneralDao : RepositorioMaestro, IGeneralDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase GeneralDao.
        /// </summary>
        public GeneralDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public async Task<bool> ActualizarFallo(string folio, string tipoOperacion, string servicio, string aliasProceso, string errDescripcion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                "dbo.usp_Lectura_MarcarPlanTrabajoConError",
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Servicio", servicio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@AliasProceso", aliasProceso));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ErrDescripcion", errDescripcion));
                });

            return r.Exito;
        }

    }
}
using Aplicacion.DTOs.ENT_SPID;
using Aplicacion.Interfaces;
using Aplicacion.Modelos;
using MUFG_T24.Aplicacion.Conversiones;
using System;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;

namespace Infraestructura.SOAP
{
    /// <summary>
    /// Representa T24 Cuenta Web Service.
    /// </summary>
    public class T24CuentaWebService : IT24CuentaWebService
    {
        private const string ClaveParametroEndpoint = "WST24_MW_ENDPOINT";
        private readonly ConfiguracionModelo _configuracion;

        /// <summary>
        /// Inicializa una nueva instancia de la clase T24CuentaWebService.
        /// </summary>
        public T24CuentaWebService(ConfiguracionModelo configuracion)
        {
            _configuracion = configuracion;
        }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public async Task<T24InformacionCuentaDto> ObtenerInformacionCuenta(string account)
        {
                string endpointWebService = ObtenerEndpointWebService();
                var wsInfoCuentaT24Ref = new wsInfoCuentaT24.T24WebServicesImplClient(
                    new BasicHttpBinding(),
                    new EndpointAddress(endpointWebService));

                var request = new wsInfoCuentaT24.VERIFICASTATUSDECUENTARequest();

                var webRequestCommon = new wsInfoCuentaT24.webRequestCommon
                {
                    company = _configuracion.StrWSCompany,
                    password = _configuracion.StrWSRegisterPassword,
                    userName = _configuracion.StrWSRegisterUserName
                };

                var enquiryInput = new wsInfoCuentaT24.enquiryInputCollection
                {
                    columnName = "ID",
                    criteriaValue = account,
                    operand = "EQ"
                };

                request.WebRequestCommon = webRequestCommon;
                request.BTMACCOUNTSTATUSType = new[]
                {
                    enquiryInput
                };

            try
            {
                var response = await wsInfoCuentaT24Ref
                    .VERIFICASTATUSDECUENTAAsync(request);

                if (response.Status.successIndicator != wsInfoCuentaT24.successIndicator.Success)
                {
                    string mensajeErrorGeneral = string.Empty;

                    if (response.Status.messages != null &&
                        response.Status.messages.Length > 0)
                    {
                        for (short i = 0; i <= response.Status.messages.Length - 1; i++)
                        {
                            mensajeErrorGeneral += response.Status.messages[i];

                            if (i != response.Status.messages.Length - 1)
                                mensajeErrorGeneral += "|";
                        }
                    }

                    throw new Exception(
                        $"El servicio regresó un status diferente a Success: {mensajeErrorGeneral}");
                }

                if (response.BTMACCOUNTSTATUSType == null ||
                    response.BTMACCOUNTSTATUSType.Length == 0)
                {
                    /// <summary>
                    /// Ejecuta la operación Exception.
                    /// </summary>
                    throw new Exception("No records were found that matched the selection criteria");
                }

                var primerResultado = response.BTMACCOUNTSTATUSType[0];
                var detalles = primerResultado.gBTMACCOUNTSTATUSDetailType?.mBTMACCOUNTSTATUSDetailType;

                if (detalles == null || detalles.Length <= 1)
                {
                    throw new Exception("La respuesta del servicio no contiene los detalles esperados para obtener el status de la cuenta.");
                }

                var detalle = detalles[1];

                if (detalle == null)
                {
                    throw new Exception("La respuesta del servicio contiene un detalle de cuenta inválido.");
                }

                return new T24InformacionCuentaDto
                {
                    Status = detalle.StatusAccount,
                    Currency = detalle.Currency,
                    Category = detalle.Category
                };
            }
            catch (Exception ex)
            {
                throw new Exception($"Error al realizar la consulta de la cuenta: {ex.Message}", ex);
            }
            finally
            {
                try
                {
                    if (wsInfoCuentaT24Ref.State == System.ServiceModel.CommunicationState.Faulted)
                        wsInfoCuentaT24Ref.Abort();
                    else
                        wsInfoCuentaT24Ref.Close();
                }
                catch
                {
                    wsInfoCuentaT24Ref.Abort();
                }
            }
    
        }

        private string ObtenerEndpointWebService()
        {
            if (_configuracion?.ParametrosSistema == null)
                throw new Exception($"No existe la colección de ParametrosSistema para obtener '{ClaveParametroEndpoint}'.");

            string endpoint;
            if (!_configuracion.ParametrosSistema.TryGetValue(ClaveParametroEndpoint, out endpoint))
            {
                endpoint = _configuracion.ParametrosSistema
                    .Where(x => string.Equals(x.Key, ClaveParametroEndpoint, StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Value)
                    .FirstOrDefault();
            }

            endpoint = ConversionesComunes.ParseTextoRequerido(endpoint, ClaveParametroEndpoint);
            return endpoint.Trim();
        }

    }
}
using Aplicacion.DTOs.SAT;
using Aplicacion.Enums;
using Aplicacion.Interfaces.SAT;
using Aplicacion.Modelos;
using Aplicacion.Modelos.SAT;
using MUFG_T24.Comunes;
using MUFG_T24.Conexiones;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Infraestructura.Repositorio;

namespace Infraestructura.Repositorio.SAT
{
    /// <summary>
    /// Representa Utilerias SAT.
    /// </summary>
    public class UtileriasSAT:RepositorioMaestro
    {

        private static uint[] table;
        private readonly ConfiguracionModelo _configuracion;
        private readonly ISATCmsDao _satCmsDao;
/// <summary>
/// Inicializa una nueva instancia de la clase UtileriasSAT.
/// </summary>

        public UtileriasSAT(BasedatosProveedor proveedor=BasedatosProveedor.SqlServer, NombreBasedatos nombreBasedatos= NombreBasedatos.TOMI, BasedatosProveedor? proveedorVistasT24=null, NombreBasedatos? nombreBasedatosVistasT24=null, ConfiguracionModelo configuracion = null) : base(proveedor, nombreBasedatos, proveedorVistasT24, nombreBasedatosVistasT24)
        {
            _configuracion = configuracion;
            _satCmsDao = new SATCmsDao();
        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>


        public async Task<string> Archivo_Validacion(string formato,string tipo_F)
        {
            string endLine = "\r\n";
            try
            {
                string nombreArchivo = $"For{formato}.001";
                string rutaBase = _configuracion?.StrValidadorSAT_Ruta ?? string.Empty;
                string rutaCompleta = string.IsNullOrWhiteSpace(rutaBase)
                    ? nombreArchivo
                    : Path.Combine(rutaBase, nombreArchivo);
                string directorio = Path.GetDirectoryName(rutaCompleta);
                if (!string.IsNullOrWhiteSpace(directorio) && !Directory.Exists(directorio))
                    Directory.CreateDirectory(directorio);
                using (StreamWriter streamWriterSAT = new StreamWriter(rutaCompleta,false,Encoding.Default)) 
                {
                    var vLecturaInicial = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_LECTURA_INICIAL_UTILERIAS_SAT, cmd =>
                    {})).Data.ToList();
                    foreach (var item in vLecturaInicial)
                    {
                        if (item.Renglon=="A1"&&item.VALOR_R?.Length==12)
                        {
                            streamWriterSAT.Write($"{item.CLAVE}={item.VALOR_R}|{endLine}");
                        }
                        else
                        {
                            streamWriterSAT.Write(item.VALOR+endLine);
                        }
                    }
                    ///Evaluacion de tipo_F
                    if (new[] { "PRV", "EJE", "CRF" }.Contains(tipo_F))
                    {
                        var vFolios = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_FOLIOS_R_UTILERIAS_SAT, cmd =>
                        {})).Data.ToList();
                        foreach (var folio in vFolios)
                        {
                            streamWriterSAT.Write(folio.VALOR + endLine);
                            var vColumnas = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                            { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", folio.Grupo)); })).Data.ToList();
                            /// <summary>
                            /// Ejecuta la operación foreach.
                            /// </summary>
                            foreach (var col in vColumnas) streamWriterSAT.Write(col.VALOR + endLine);
                        }

                        var vFoliosA = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_FOLIOS_A_UTILERIAS_SAT, cmd =>
                        { })).Data.ToList();
                        /// <summary>
                        /// Ejecuta la operación foreach.
                        /// </summary>
                        foreach (var itemA in vFoliosA) streamWriterSAT.Write(itemA.VALOR + endLine);

                    }
                    else if (tipo_F=="DPA")
                    {
                        var vConteoR23 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_CONTEO_R23_UTILERIAS_SAT, cmd =>
                        { })).Data.ToList();

                        if (vConteoR23!=null && vConteoR23.Count>0 && vConteoR23[0].Conteo > 0)
                        {
                            var vListaR23 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_R23_UTILERIAS_SAT, cmd =>
                            { })).Data.ToList();

                            foreach (var r23 in vListaR23)
                            {
                                streamWriterSAT.Write(r23.VALOR + endLine);
                                var vColumnas = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                                { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", r23.Grupo)); })).Data.ToList();
/// <summary>
/// Ejecuta la operación foreach.
/// </summary>

                                foreach (var col in vColumnas) streamWriterSAT.Write(col.VALOR + endLine);                               
                            }
                        }
                        ///NEPE v5.0
                        
                        var vListaClaves = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_CLAVES_14704_UTILERIAS_SAT, cmd =>
                        { })).Data.ToList();
                        foreach (var clave in vListaClaves)
                        {
                           var vColumnasR47 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_R47_UTILERIAS_SAT, cmd =>
                            { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", clave.Grupo)); })).Data.ToList();
                            /// <summary>
                            /// Ejecuta la operación foreach.
                            /// </summary>
                            foreach (var col in vColumnasR47) streamWriterSAT.Write(col.VALOR + endLine);

                            var vColumnasC = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                            { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", clave.Grupo)); })).Data.ToList();
                            /// <summary>
                            /// Ejecuta la operación foreach.
                            /// </summary>
                            foreach (var col in vColumnasC) streamWriterSAT.Write(col.VALOR + endLine);
                        }
                    }
                    else
                    {
                        var vListaElse = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_CLAVES_14704_UTILERIAS_SAT, cmd =>
                        { })).Data.ToList();
                        foreach (var clave in vListaElse)
                        {
                            var vColumnas = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                            { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", clave.Grupo)); })).Data.ToList();
                            /// <summary>
                            /// Ejecuta la operación foreach.
                            /// </summary>
                            foreach (var col in vColumnas) streamWriterSAT.Write(col.VALOR + endLine);                            
                        }
                    }

                    var vSalidaFinal1 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_FINALES_1_UTILERIAS_SAT, cmd =>
                    {})).Data.ToList();
                    /// <summary>
                    /// Ejecuta la operación foreach.
                    /// </summary>
                    foreach (var col in vSalidaFinal1) streamWriterSAT.Write(col.VALOR + endLine);

                    var vSalidaFinal16 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_A16_UTILERIAS_SAT, cmd =>
                    { })).Data.ToList();
                    /// <summary>
                    /// Ejecuta la operación foreach.
                    /// </summary>
                    foreach (var col in vSalidaFinal16) streamWriterSAT.Write(col.VALOR + endLine);

                    streamWriterSAT.Write("999999|" + endLine);
                    streamWriterSAT.Write("EOF");

                }
                return nombreArchivo;
            }
            catch (Exception e)
            {
                Config.EscribirLog($"ERROR|Archivo_Validacion|Ocurre Excepcion:{e}");
                return null;
            }
        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>

        public async Task<string> Archivo_Envio(string tipo_F)
        {           
            string returnLine = "";

            //Se inicia el ciclo de lectura
            var vLecturaInicial = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_LECTURA_INICIAL_UTILERIAS_SAT, cmd =>
            {                
            })).Data.ToList();

            foreach (var item in vLecturaInicial)
            {
                returnLine += (item.Renglon == "A1" && item.VALOR_R?.Length == 12) ? $"{item.CLAVE}={item.VALOR_R}|" : item.VALOR;
            }

            if (new[] { "PRV", "EJE", "CRF" }.Contains(tipo_F))
            {
                var vFolios = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_FOLIOS_R_UTILERIAS_SAT, cmd =>
                {
                })).Data.ToList();

                foreach (var folio in vFolios)
                {
                    returnLine += folio.VALOR;
                    var vColumnas = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", folio.Grupo));
                    })).Data.ToList();

                    foreach (var col in vColumnas) returnLine += col.VALOR;
                }
                var vFoliosA = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_FOLIOS_A_UTILERIAS_SAT, cmd =>
                { })).Data.ToList();
                foreach (var itemA in vFoliosA) returnLine += itemA.VALOR;
            }
            else if(tipo_F == "DPA")
            {
                var vConteoR23 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_CONTEO_R23_UTILERIAS_SAT, cmd =>
                {
                })).Data.ToList();

                if (vConteoR23!=null && vConteoR23.Count>0 && vConteoR23[0].Conteo > 0)
                {
                    var vListaR23 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_R23_UTILERIAS_SAT, cmd =>
                    {})).Data.ToList();

                    foreach (var r23 in vListaR23)
                    {
                        returnLine += r23.VALOR;
                        var vColumnas = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                        { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", r23.Grupo)); })).Data.ToList();

                        foreach (var col in vColumnas) returnLine += col.VALOR;
                        
                    }
                }
                //NEPE v5.0
                var vListasClaves = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_CLAVES_14704_UTILERIAS_SAT, cmd =>
                {})).Data.ToList();

                foreach (var clave in vListasClaves)
                {
                    var vColumnasR47 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_R47_UTILERIAS_SAT, cmd =>
                    { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", clave.Grupo)); })).Data.ToList();
                    foreach (var col in vColumnasR47) returnLine += col.VALOR;
                    var vColumnasC = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_UTILERIAS_SAT, cmd =>
                    { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", clave.Grupo)); })).Data.ToList();
                    foreach (var col in vColumnasC) returnLine += col.VALOR;
                }
            }
            else
            {
                var vlistaElse = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_CLAVES_14704_UTILERIAS_SAT, cmd =>
                {})).Data.ToList();

                foreach (var item in vlistaElse)
                {
                    var vColumnasC = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_COLUMNAS_C_ALL_UTILERIAS_SAT, cmd =>
                    { cmd.Parameters.Add(basedatosConexion.CrearParametro("@grupo", item.Grupo)); })).Data.ToList();
                    foreach (var col in vColumnasC) returnLine += col.VALOR;
                }
            }

            var vSalidaFinal1 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_FINALES_1_UTILERIAS_SAT, cmd =>
            {})).Data.ToList();
            foreach (var item in vSalidaFinal1) returnLine += item.VALOR;

            var vSalidaFinal16 = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_A16_UTILERIAS_SAT, cmd =>
            { })).Data.ToList();
            foreach (var item in vSalidaFinal16) returnLine += item.VALOR;

            return returnLine + "999999|";
        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>

        public async Task<string> Calcula_Fecha_Recaudacion(string fecha_Presentacion, string hora_Presentacion)
        {
            TimeSpan horaLimite = _configuracion?.StrHoraCierreSAT ?? TimeSpan.Parse("13:30");
            TimeSpan horaPresentacionObj=TimeSpan.Parse(hora_Presentacion);
            if (horaLimite >= horaPresentacionObj)
            {
                return fecha_Presentacion;
            }
            else 
            {
                /// <summary>
                /// Ejecuta la operación Dia_Habil_Siguiente.
                /// </summary>
                return await Dia_Habil_Siguiente(fecha_Presentacion,1);
            }
                
        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>

        public async Task<SAT_DatosRegresoModelo> CMS_NEPE_A_TOMI(string folio, string folioUnico)
        {
            var datosRegreso = new SAT_DatosRegresoModelo
            {
                Resultado = false,
                ResultadoValidacion = 0,
                ValoresRegreso = new SAT_ValoresFormato_Modelo()
            };

            try
            {
                SAT_Formato_DTO formato = await ObtenerFormatoSatDetallePorFolio(folio);
                if (formato == null)
                    return datosRegreso;

                SAT_DTO basico = await ObtenerFormatoSatBasicoPorFolio(folio);
                string customerCode = basico?.Cdl.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                string debitAccount = basico?.Id_cuenta.ToString(CultureInfo.InvariantCulture) ?? string.Empty;

                string tipoFormato = GetTipoFormatoNepe(formato.TipoFormato);
                if (string.IsNullOrWhiteSpace(tipoFormato))
                    return datosRegreso;

                DateTime? fechaAutorizacion = (formato.Id_Autorizaciones == 2 && formato.FechaAutorizacion_dos.HasValue)
                    ? formato.FechaAutorizacion_dos
                    : formato.FechaAutorizacion;

                string fechaPresentacion = fechaAutorizacion?.ToString("yyyyMMdd") ?? string.Empty;
                string horaPresentacion = fechaAutorizacion?.ToString("HH:mm") ?? "00:00";
                string fechaRecaudacion = string.IsNullOrEmpty(fechaPresentacion)
                    ? string.Empty
                    : await Calcula_Fecha_Recaudacion(fechaPresentacion, horaPresentacion);

                bool appletDisponible = await TieneSalidaApplet();
                string resultadoValidador = "00000010";
                string archivoValidador = appletDisponible ? await Archivo_Validacion(folio, tipoFormato) : string.Empty;
                bool ignorarValidador = string.Equals(_configuracion?.StrValidadorSAT_IgnoraValidador, "1", StringComparison.Ordinal);
                if (ignorarValidador)
                    resultadoValidador = "0000000";
                else if (!string.IsNullOrWhiteSpace(archivoValidador))
                    resultadoValidador = Validador(archivoValidador);
                else if (!appletDisponible)
                    resultadoValidador = "0000000";

                int resultadoValidacion = 1;
                if (!string.IsNullOrWhiteSpace(resultadoValidador) && resultadoValidador.Length >= 7)
                    resultadoValidacion = (resultadoValidador[6] == '0') ? 0 : 1;

                if (resultadoValidacion != 0)
                {
                    datosRegreso.Resultado = false;
                    datosRegreso.ResultadoValidacion = 1;
                    return datosRegreso;
                }

                try
                {
                    string rutaBaseVal = _configuracion?.StrValidadorSAT_Ruta ?? string.Empty;
                    string rutaArchivoVal = string.IsNullOrWhiteSpace(rutaBaseVal)
                        ? archivoValidador
                        : Path.Combine(rutaBaseVal, archivoValidador ?? string.Empty);
                    if (!string.IsNullOrWhiteSpace(rutaArchivoVal) && File.Exists(rutaArchivoVal))
                        File.Delete(rutaArchivoVal);
                }
                catch
                {
                }

                string fechaConcentracion = string.IsNullOrWhiteSpace(fechaRecaudacion)
                    ? string.Empty
                    : await Dia_Habil_Siguiente(fechaRecaudacion, 3);

                string fechaPago = string.IsNullOrWhiteSpace(fechaRecaudacion)
                    ? string.Empty
                    : await Dia_Habil_Siguiente(fechaRecaudacion, 2);

                var valoresXml = ParseValoresFormatoXml(formato.FormatoXML);

                string monto = formato.MontoOperacion.ToString(CultureInfo.InvariantCulture);
                if (valoresXml.ContainsKey("10017") && !string.IsNullOrWhiteSpace(valoresXml["10017"]))
                    monto = valoresXml["10017"];

                valoresXml["10017"] = monto;
                valoresXml["40002"] = fechaPresentacion;
                valoresXml["20005"] = fechaRecaudacion;

                string nombreLlave =
                    NormalizeNoSpacesUpper(GetDictValue(valoresXml, "10003")) +
                    NormalizeNoSpacesUpper(GetDictValue(valoresXml, "10004")) +
                    NormalizeNoSpacesUpper(GetDictValue(valoresXml, "10005")) +
                    /// <summary>
                    /// Ejecuta la operación NormalizeNoSpacesUpper.
                    /// </summary>
                    NormalizeNoSpacesUpper(GetDictValue(valoresXml, "10006"));
                string efectivamentePagado = (monto ?? string.Empty).Replace(" ", string.Empty);
                string llavePago = string.IsNullOrWhiteSpace(fechaPresentacion)
                    ? string.Empty
                    : Llave_Pago(fechaPresentacion, nombreLlave, efectivamentePagado, folio);

                string cadenaOriginal = !string.IsNullOrWhiteSpace(formato.CadenaOriginal)
                    ? formato.CadenaOriginal
                    : (formato.CadenaOriginalApplet ?? string.Empty);
                if (string.IsNullOrWhiteSpace(cadenaOriginal))
                    cadenaOriginal = ConstruirCadenaOriginalFallback(BuildArchivoSat(valoresXml, "10017", "40002", "20005"));

                string selloDigital = !string.IsNullOrWhiteSpace(formato.ContenidoSelloDigit)
                    ? formato.ContenidoSelloDigit
                    : GenerarSelloDigital(cadenaOriginal);

                if (string.IsNullOrWhiteSpace(selloDigital))
                {
                    datosRegreso.Resultado = false;
                    datosRegreso.ResultadoValidacion = 1;
                    return datosRegreso;
                }
/// <summary>
/// Ejecuta la operación ActualizarSelloSalidaApplet.
/// </summary>

                await ActualizarSelloSalidaApplet(selloDigital);

                string archivoSat = appletDisponible ? await Archivo_Envio(tipoFormato) : string.Empty;
                if (string.IsNullOrWhiteSpace(archivoSat))
                    archivoSat = BuildArchivoSat(valoresXml, "10017", "40002", "20005");

                if (string.IsNullOrWhiteSpace(cadenaOriginal))
                    cadenaOriginal = ConstruirCadenaOriginalFallback(archivoSat);

                bool insertado = await InsertarFormatoCmsSatLegacy(
                    tipoFormato,
                    folio,
                    folioUnico,
                    customerCode,
                    debitAccount,
                    string.Empty,
                    formato.FechaCaptura,
                    formato.Id_UsuarioCaptura,
                    fechaAutorizacion,
                    formato.Id_UsuarioAutorizo,
                    formato.MontoOperacion,
                    formato.ContenidoFormato,
                    formato.FormatoOriginalCompleto,
                    cadenaOriginal,
                    selloDigital,
                    archivoSat,
                    "PEND",
                    fechaConcentracion,
                    fechaPago,
                    formato.Descripcion,
                    _configuracion.StrFechaSistema,
                    ParseIntSafe(fechaRecaudacion),
                    llavePago);

                datosRegreso.Resultado = insertado;
                datosRegreso.ResultadoValidacion = resultadoValidacion;
                datosRegreso.ValoresRegreso = new SAT_ValoresFormato_Modelo
                {
                    ArchivoSat = archivoSat,
                    CadenaOriginal = cadenaOriginal,
                    SelloDigital = selloDigital,
                    LLavePago = llavePago,
                    FechaPresentacion = fechaPresentacion,
                    FechaRecaudacion = fechaRecaudacion,
                    FechaEjecucion = ParseIntSafe(fechaRecaudacion)
                };
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|SAT.CMS_NEPE_A_TOMI|Se presenta exception: {ex}");
            }

            return datosRegreso;
        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>

        public async Task<SAT_DatosRegresoModelo> CMS_DREF_A_TOMI(string folio, string folioUnico)
        {
            var datosRegreso = new SAT_DatosRegresoModelo
            {
                Resultado = false,
                ResultadoValidacion = 0,
                ValoresRegreso = new SAT_ValoresFormato_Modelo()
            };

            try
            {
                SAT_Formato_DTO formato = await ObtenerFormatoSatDetallePorFolio(folio);
                if (formato == null)
                    return datosRegreso;

                SAT_DTO basico = await ObtenerFormatoSatBasicoPorFolio(folio);
                string customerCode = basico?.Cdl.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
                string debitAccount = basico?.Id_cuenta.ToString(CultureInfo.InvariantCulture) ?? string.Empty;

                DateTime? fechaAutorizacion = (formato.Id_Autorizaciones == 2 && formato.FechaAutorizacion_dos.HasValue)
                    ? formato.FechaAutorizacion_dos
                    : formato.FechaAutorizacion;

                string fechaPresentacion = fechaAutorizacion?.ToString("yyyyMMdd") ?? string.Empty;
                string horaPresentacion = fechaAutorizacion?.ToString("HH:mm") ?? "00:00";
                string fechaRecaudacion = string.IsNullOrEmpty(fechaPresentacion)
                    ? string.Empty
                    : await Calcula_Fecha_Recaudacion(fechaPresentacion, horaPresentacion);

                if (!string.Equals(fechaRecaudacion, _configuracion.StrFechaSistema, StringComparison.Ordinal))
                    return datosRegreso;

                string fechaConcentracion = await Dia_Habil_Siguiente(fechaRecaudacion, 3);
                string fechaPago = await Dia_Habil_Siguiente(fechaRecaudacion, 2);

                string[] valoresFormato = SplitContenidoFormatoRequerido(formato.ContenidoFormato, 18, folio);
                string lineaCaptura = ExtractContenidoFormatoValor(valoresFormato, 3);
                string nombreCuenta = ExtractContenidoFormatoValor(valoresFormato, 16);
                string montoPago = Convert.ToInt64(formato.MontoOperacion).ToString(CultureInfo.InvariantCulture);
                string llavePago = string.Empty;
                if (!string.IsNullOrWhiteSpace(lineaCaptura) && !string.IsNullOrWhiteSpace(fechaRecaudacion))
                    llavePago = calculo_llavePagoDRE(folio, lineaCaptura, fechaRecaudacion, montoPago);

                bool insertado = await InsertarFormatoCmsSatLegacy(
                    "DRE",
                    folio,
                    folioUnico,
                    customerCode,
                    debitAccount,
                    nombreCuenta,
                    formato.FechaCaptura,
                    formato.Id_UsuarioCaptura,
                    fechaAutorizacion,
                    formato.Id_UsuarioAutorizo,
                    formato.MontoOperacion,
                    lineaCaptura,
                    lineaCaptura,
                    lineaCaptura,
                    lineaCaptura,
                    lineaCaptura,
                    "PEND",
                    fechaConcentracion,
                    fechaPago,
                    formato.Descripcion,
                    fechaPresentacion,
                    ParseIntSafe(fechaRecaudacion),
                    llavePago);

                datosRegreso.Resultado = insertado;
                datosRegreso.ResultadoValidacion = 0;
                datosRegreso.ValoresRegreso = new SAT_ValoresFormato_Modelo
                {
                    FechaPresentacion = fechaPresentacion,
                    FechaRecaudacion = fechaRecaudacion,
                    FechaEjecucion = ParseIntSafe(fechaRecaudacion),
                    LLavePago = llavePago,
                    ArchivoSat = string.Empty,
                    CadenaOriginal = formato.CadenaOriginal ?? string.Empty,
                    SelloDigital = formato.ContenidoSelloDigit ?? string.Empty
                };
            }
            catch (Exception ex)
            {
                Config.EscribirLog($"ERROR|SAT.CMS_DREF_A_TOMI|Se presenta exception: {ex}");
            }

            return datosRegreso;
        }

        private async Task<SAT_Formato_DTO> ObtenerFormatoSatDetallePorFolio(string folio)
        {
            return await _satCmsDao.ObtenerFormatoSatDetallePorFolio(folio);
        }

        private async Task<SAT_DTO> ObtenerFormatoSatBasicoPorFolio(string folio)
        {
            return await _satCmsDao.ObtenerFormatoSatPorFolio(folio);
        }

        private async Task<bool> InsertarFormatoCmsSatLegacy(
            string nfTipo,
            string nfFolio,
            string nfFolioUnico,
            string t24CustomerCode,
            string t24DebitAcctNo,
            string t24CompanyName,
            DateTime? nfFechaCaptura,
            string nfUsuarioCaptura,
            DateTime? nfFechaAutoriza,
            string nfUsuarioAutoriza,
            decimal t24DebitAmount,
            string nfContenidoFormato,
            string nfFormatoOriginalCompleto,
            string nfCadenaOriginal,
            string nfSelloDigital,
            string nfArchivoSat,
            string nfEstatus,
            string nfFechaConcentracion,
            string nfFechaPago,
            string nfDescripcion,
            string nfFechaRecaudacion,
            int nfFechaEjecucion,
            string nfLlavePago)
        {
            var salida = basedatosConexion.CrearParametro("@Salida", 1);
            salida.DbType = DbType.Int32;
            salida.Direction = ParameterDirection.Output;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                "dbo.usp_tombin_insert_formato_cms_sat",
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO", nfTipo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO", ParseLongSafe(nfFolio)));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", t24CustomerCode));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", t24DebitAcctNo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_COMPANY_NAME", t24CompanyName));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CAPTURA", nfFechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_CAPTURA", nfUsuarioCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_AUTORIZA", nfFechaAutoriza));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_AUTORIZA", nfUsuarioAutoriza));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", t24DebitAmount));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CONTENIDO_FORMATO", nfContenidoFormato));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FORMATO_ORIGINAL_COMPLETO", nfFormatoOriginalCompleto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CADENA_ORIGINAL", nfCadenaOriginal));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_SELLO_DIGITAL", nfSelloDigital));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ARCHIVO_SAT", nfArchivoSat));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", nfEstatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CONCENTRACION", nfFechaConcentracion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_PAGO", nfFechaPago));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_DESCRIPCION", nfDescripcion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_RECAUDACION", nfFechaRecaudacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", nfFechaEjecucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_LLAVE_PAGO", nfLlavePago));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", nfFolioUnico));
                    cmd.Parameters.Add(salida);
                });

            return r.Exito && OutputParameterHelper.EsSalidaCero(salida, "dbo.usp_tombin_insert_formato_cms_sat");
        }

        private static string GetTipoFormatoNepe(long tipoFormato)
        {
            switch (tipoFormato)
            {
                case 5: return "EJE";
                case 6: return "PRV";
                case 9: return "CRF";
                case 10: return "DPA";
                default: return string.Empty;
            }
        }

        private async Task ActualizarSelloSalidaApplet(string selloDigital)
        {
            if (string.IsNullOrWhiteSpace(selloDigital))
                return;

            await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                "dbo.usp_LecturaSAT_ActualizarSelloSalidaApplet",
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SelloDigital", selloDigital));
                });
        }

   
        private static int ParseIntSafe(string value)
        {
            int.TryParse(value, out int n);
            return n;
        }

        private static long ParseLongSafe(string value)
        {
            long.TryParse(value, out long n);
            return n;
        }

        private static string ConstruirCadenaOriginalFallback(string archivoSat)
        {
            if (string.IsNullOrWhiteSpace(archivoSat))
                return string.Empty;

            string normalizado = archivoSat.Trim('|');
            if (string.IsNullOrWhiteSpace(normalizado))
                return string.Empty;

            return "||" + normalizado + "|";
        }

        private string GenerarSelloDigital(string cadenaOriginal)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(cadenaOriginal))
                    return string.Empty;

                string certPath = _configuracion?.StrValidadorSAT_CertPathName;
                string certPwd = _configuracion?.StrValidadorSAT_CertPsw;
                if (string.IsNullOrWhiteSpace(certPath) || !File.Exists(certPath))
                    return string.Empty;

                var cert = string.IsNullOrWhiteSpace(certPwd)
                    ? new X509Certificate2(certPath)
                    : new X509Certificate2(certPath, certPwd);

                using (RSA rsa = cert.GetRSAPrivateKey())
                {
                    if (rsa == null)
                        return string.Empty;

                    byte[] data = Encoding.Default.GetBytes(cadenaOriginal);
                    byte[] signature = rsa.SignData(data, HashAlgorithmName.MD5, RSASignaturePadding.Pkcs1);
                    return Convert.ToBase64String(signature);
                }
            }
            catch
            {
                return string.Empty;
            }
        }

        private static Dictionary<string, string> ParseValoresFormatoXml(string formatoXml)
        {
            var valores = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(formatoXml))
                return valores;

            formatoXml = NormalizarXmlLegacySat(formatoXml);

            var matches = Regex.Matches(
                formatoXml,
                "<CONCEPTO>.*?<CLAVE>(.*?)</CLAVE>.*?<VALOR>(.*?)</VALOR>.*?</CONCEPTO>",
                RegexOptions.Singleline | RegexOptions.IgnoreCase);

            foreach (Match match in matches)
            {
                if (!match.Success || match.Groups.Count < 3)
                    continue;

                string clave = (match.Groups[1].Value ?? string.Empty).Trim();
                string valor = (match.Groups[2].Value ?? string.Empty).Trim();
                if (!string.IsNullOrWhiteSpace(clave))
                    valores[clave] = System.Net.WebUtility.HtmlDecode(valor);
            }

            return valores;
        }

        private static string NormalizarXmlLegacySat(string xml)
        {
            if (string.IsNullOrWhiteSpace(xml))
                return string.Empty;

            string t = xml;
            t = t.Replace("&#x21;", "!")
                 .Replace("&#x22;", "\"")
                 .Replace("&#x23;", "#")
                 .Replace("&#x24;", "$")
                 .Replace("&#x26;", "&")
                 .Replace("&apos;", "'")
                 .Replace("&#x27;", "'")
                 .Replace("&#x28;", "(")
                 .Replace("&#x29;", ")")
                 .Replace("&#x2f;", "/")
                 .Replace("&#x3c;", "<")
                 .Replace("&#x3e;", ">")
                 .Replace("&#x3f;", "?")
                 .Replace("&#x40;", "@")
                 .Replace("&#x5b;", "[")
                 .Replace("&#x5d;", "]")
                 .Replace("&#x5f;", "_")
                 .Replace("&#x7b;", "{")
                 .Replace("&#x7d;", "}")
                 .Replace("&#xa1;", "¡")
                 .Replace("&#xb0;", "°")
                 .Replace("&#xbf;", "¿")
                 .Replace("&#xc1;", "Á")
                 .Replace("&#xc9;", "É")
                 .Replace("&#xcd;", "Í")
                 .Replace("&#xd1;", "Ñ")
                 .Replace("&#xd3;", "Ó")
                 .Replace("&#xda;", "Ú")
                 .Replace("&#xe1;", "á")
                 .Replace("&#xe9;", "é")
                 .Replace("&#xed;", "í")
                 .Replace("&#xf1;", "ñ")
                 .Replace("&#xf3;", "ó")
                 .Replace("&#xfa;", "ú")
                 .Replace("&#x2e;", ".")
                 .Replace("&#xfc;", "ü");
            return t;
        }

        private async Task<bool> TieneSalidaApplet()
        {
            try
            {
                var data = (await EjecutarStoredProcedureQuery<SALIDAAPPLET_UTILERIAS_SAT_DTO>(
                    ProcedimientoAlmacenado.SP_GET_SALIDAAPPLET_LECTURA_INICIAL_UTILERIAS_SAT,
                    cmd => { })).Data;

                return data != null && data.Any();
            }
            catch
            {
                return false;
            }
        }

        private static string BuildArchivoSat(Dictionary<string, string> valores, params string[] claves)
        {
            var partes = new StringBuilder();
            foreach (var clave in claves)
            {
                string valor = valores.ContainsKey(clave) ? valores[clave] : string.Empty;
                if (partes.Length > 0)
                    partes.Append('|');
                partes.Append(clave).Append('=').Append(valor ?? string.Empty);
            }
            return partes.ToString();
        }

        private static string GetDictValue(Dictionary<string, string> valores, string clave)
        {
            if (valores == null || string.IsNullOrWhiteSpace(clave))
                return string.Empty;

            return valores.ContainsKey(clave) ? (valores[clave] ?? string.Empty) : string.Empty;
        }

        private static string NormalizeNoSpacesUpper(string valor)
        {
            return string.IsNullOrWhiteSpace(valor)
                ? string.Empty
                : valor.ToUpperInvariant().Replace(" ", string.Empty);
        }

        private static string[] SplitContenidoFormatoRequerido(string contenidoFormato, int minIndex, string folio)
        {
            string[] pipes = (contenidoFormato ?? string.Empty).Split('|');
            if (pipes.Length - 1 < minIndex)
                throw new Exception($"El contenidoFormato del folio {folio} es diferente al esperado, debe ser {minIndex} datos y se tienen {pipes.Length - 1}.");

            return pipes;
        }

        private static string ExtractContenidoFormatoValor(string[] pipes, int index)
        {
            if (pipes == null || index < 0 || index >= pipes.Length)
                return string.Empty;

            string[] llaveValor = pipes[index].Split('=');
            if (llaveValor.Length < 2)
                return string.Empty;

            return llaveValor[1]?.Trim() ?? string.Empty;
        }

        private async Task<string> Dia_Habil_Siguiente(string Fecha_Presentacion,int nDias)
        {         
            try
            {
                DateTime Variable_FechaDT;
                if (Fecha_Presentacion.Length==8)
                {
                    Variable_FechaDT = DateTime.ParseExact(Fecha_Presentacion,"yyyyMMdd",CultureInfo.InvariantCulture);
                }
                else
                {
                   Variable_FechaDT=DateTime.Parse(Fecha_Presentacion);
                }

                async Task<bool> EsDiaInHabil(DateTime fechaEvaluar)
                {
                    var response = (await EjecutarStoredProcedureQuery<CONTEO_DIAS_INHABIL_SAT_DTO>(ProcedimientoAlmacenado.SP_GET_DIAINHABIL_UTILERIAS_SAT, cmd =>
                    {
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@dianohabil", fechaEvaluar));
                        cmd.Parameters.Add(basedatosConexion.CrearParametro("@tipoccy", "MXN"));
                    })).Data?.FirstOrDefault();
                    return response != null && response.Conteo > 0;
                }
                for (int i = 0; i < nDias; i++)
                {
                    Variable_FechaDT = Variable_FechaDT.AddDays(1);
                    while (Variable_FechaDT.DayOfWeek == DayOfWeek.Saturday || Variable_FechaDT.DayOfWeek == DayOfWeek.Sunday|| await EsDiaInHabil(Variable_FechaDT))
                    {
                        Variable_FechaDT = Variable_FechaDT.AddDays(1);
                    }
                }
                return Variable_FechaDT.ToString("yyyyMMdd");

            }
            catch (Exception)
            {
                   
                throw;
            }        
        }

        private static string Llave_Pago(string fecha,string nombre,string efectivamentePago,string numeroOperacion)
        {
            string cadena1 = "S" + numeroOperacion + nombre + "T" + fecha + efectivamentePago + "A";
            long[] tabla = new long[256];
            for (int i = 0; i <256; i++) 
            {
                long x = i;
                for (int j = 0; j < 8; j++)
                {
                    if ((x&1)==1)
                    {
                        x = (x >> 1) ^ 3988292384;
                    }
                    else
                    {
                        x = x >> 1;
                    }

                }
                tabla[i] = x;
            }
            long resultado = Calcula_crc32(cadena1, tabla);
            string cadena2 = resultado.ToString("x8");
            int mesInt = int.Parse(fecha.Substring(4, 2));
            string cadena3 = mesInt.ToString("X");
            string resultado_digito = Obtener_Digito_Verificador(cadena2+cadena3);
            return cadena2 + cadena3 + resultado_digito;
        }

        private static string Obtener_Digito_Verificador(string cadena) 
        {
            int y = 0;
            for (int i = 0; i <= 8; i++)
            {
                int valor_decimal = cadena[i];
                y += valor_decimal * (13 - i);
            }
            int result = y % 11;
            if (result == 0) return "0";
            if (result == 1) return "1";
/// <summary>
/// Ejecuta la operación return.
/// </summary>

            return ((char)(48 + (11 - result))).ToString();
        }

        private static long Calcula_crc32(string cadena, long[] tabla)
        {
            long x = 4294967295;
            for (int i = 0; i < cadena.Length; i++) 
            {
                int valor_decimal = cadena[i];
                x = (x >> 8) ^ tabla[(int)((x ^ valor_decimal) & 255)];
            }
            return x ^ 4294967295;
        }

        private string Validador(string archivo)
        {
            string gsValidadorSAT_servidor = _configuracion?.StrValidadorSAT_Servidor;
            int gsValidadorSAT_puerto = ParseIntSafe(_configuracion?.StrValidadorSAT_Puerto);
            int SOCKET_TIMEOUT = 30000;
            TcpClient loTCP_Client;
            NetworkStream loNetworkStream;
            byte[] laSendBytes;
            byte[] laReadBytes = new byte[1024];
            string lsCadenaEnvio;
            StringBuilder lsCompleteMessage;
            int liNumberOfBytesRead;
            string lsReturnData;
            Config.EscribirLog($"INFO|Inicio proceso de validacion del SAT, archivo = {archivo} en {gsValidadorSAT_servidor} : {gsValidadorSAT_puerto}.");
            if (string.IsNullOrWhiteSpace(gsValidadorSAT_servidor) || gsValidadorSAT_puerto <= 0)
            {
                return "00000010";
            }
            loTCP_Client= new TcpClient();
            loTCP_Client.Connect(gsValidadorSAT_servidor, gsValidadorSAT_puerto);
            loNetworkStream= loTCP_Client.GetStream();
            if (loNetworkStream.CanWrite && loNetworkStream.CanRead)
            {
                lsCompleteMessage = new StringBuilder();
                lsCadenaEnvio = "00000002" + archivo;

                laSendBytes= Encoding.ASCII.GetBytes(lsCadenaEnvio);

                loNetworkStream.Write(laSendBytes, 0, laSendBytes.Length);

                Config.EscribirLog($"INFO|Envio de informacion = {lsCadenaEnvio}, esperando respuesta.");

                loNetworkStream.ReadTimeout = SOCKET_TIMEOUT;
                liNumberOfBytesRead = 0;

                do
                {
                    liNumberOfBytesRead=loNetworkStream.Read(laReadBytes,0,laReadBytes.Length);
                    lsCompleteMessage.AppendFormat("{0}",Encoding.ASCII.GetString(laReadBytes,0,liNumberOfBytesRead));
                } 
                /// <summary>
                /// Ejecuta la operación while.
                /// </summary>
                while (loNetworkStream.DataAvailable);

                lsReturnData=lsCompleteMessage.ToString();

                Config.EscribirLog($"INFO|Validador|Regreso de respuesta= {lsReturnData}");

                return lsReturnData;

            }
            return "00000010";
        }

        private static void llena_tabla()
        {
            uint poly = 0xEDB88320;
            table = new uint[256];
            for (int i = 0; i < table.Length; i++)
            {
                uint temp = (uint)i;
                for (int j = 8;  j >=1; j--)
                {
                    if ((temp&1)==1)
                    {
                        temp = (temp >> 1) ^ poly;
                    }
                    else
                    {
                        temp >>= 1;
                    }
                }

                table[i] = temp;
            }
        }

        private  static string calculo_llavePagoDRE(string folio,string lineaCaptura,string fecha, string monto)
        {
            string lsCadena = "S" + folio + lineaCaptura + "T" + fecha + monto + "A";
            /// <summary>
            /// Ejecuta la operación llena_tabla.
            /// </summary>
            llena_tabla();
            uint liCheckSum = ComputeChecksum(System.Text.Encoding.ASCII.GetBytes(lsCadena));
            string lsCadena2 = liCheckSum.ToString("X8");
            int mesInt = int.Parse(fecha.Substring(4, 2));
            string lsMes = mesInt.ToString("X");
            string lsCadena3= lsCadena2 + lsMes;
            char lsDigito = Digito_verificador(lsCadena3);
            return lsCadena3+lsDigito;

        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>

        public static uint ComputeChecksum(byte[] bBytes)
        {
            uint crc = 0xFFFFFFFF;
            for (int i = 0; i < bBytes.Length; i++)
            {
                byte index = (byte)((crc & 0xFF) ^ bBytes[i]);
                crc = (crc >> 8) ^ table[index];
            }
            return ~crc;
        }
/// <summary>
/// Ejecuta la operación public.
/// </summary>


        public static char Digito_verificador(string strCadena)
        {
            int y = 0;
            //iteramos la posiciones 
            for (int i = 0; i <= 8; i++)
            {
                int valorDecimal = strCadena[i];
                y += valorDecimal * (13 - i);
            }
            int result = y % 11;
            if (result == 0) return '0';
            if (result == 1) return '1';
            /// <summary>
            /// Ejecuta la operación return.
            /// </summary>
            return (char)('0' + (11 - result));
        }

    }
}
using Aplicacion.DTOs.CNT;
using Aplicacion.Enums;
using Aplicacion.Interfaces.CNT;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.CNT
{
    /// <summary>
    /// Representa CNTDao.
    /// </summary>
    public class CNTDao : RepositorioMaestro, ICNTDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase CNTDao.
        /// </summary>
        public CNTDao()
        : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos de la concentradora asociados al folio solicitado.
        /// </summary>
        public async Task<CntConcentradoraDto> ObtenerConcentradoraPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<CntConcentradoraDto>(
                ConstantesServicio.ProcedimientosCNT.USP_LECTURA_CNT_OBTENER_CONCENTRADORA_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el folio único de la concentradora asociada al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarFolioUnicoConcentradora(string folio, string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosCNT.USP_LECTURA_CNT_ACTUALIZAR_FOLIO_UNICO_CONCENTRADORA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza datos del plan de trabajo maestro a partir de la información de la concentradora asociada al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarDatosPlanTrabajoMaestroDesdeConcentradora(
            string folio,
            string tipoOperacion,
            string estatus,
            string folioUnico,
            int fechaValor,
            string monedaDefaultMn)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosCNT.USP_LECTURA_CNT_ACTUALIZAR_DATOS_PLAN_TRABAJO_MAESTRO_DESDE_CONCENTRADORA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Estatus", estatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cuenta los registros de entradas y salidas asociados a la referencia solicitada.
        /// </summary>
        public async Task<int> ContarEntradasSalidasPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosCNT.USP_LECTURA_CNT_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Obtiene los datos de entrada y salida asociados a la referencia solicitada.
        /// </summary>
        public async Task<CntEntradaSalidaDto> ObtenerEntradaSalidaPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<CntEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosCNT.USP_LECTURA_CNT_OBTENER_ENTRADA_SALIDA_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalidaSrv(
            string referencia,
            int numCliente,
            double cdl,
            string ordenante,
            double operacion,
            string operacionDesc,
            string tipo,
            string tipoDesc,
            double cuenta,
            string nombre,
            string cuentaMoneda,
            double via,
            string viaDesc,
            decimal importe,
            decimal saldo,
            string comentarios,
            DateTime fechaCaptura,
            DateTime fechaValor,
            string hora,
            string capturista,
            double fad,
            string referenciaControl,
            double referenciaControlCont)
        {

            IDbDataParameter salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosCNT.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", cdl));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", ordenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", operacionDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", tipo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", tipoDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", nombre));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", cuentaMoneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", viaDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", saldo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", comentarios));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", fechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", hora));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", capturista));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", referenciaControl));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", referenciaControlCont));


                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && Convert.ToInt32(salidaParam?.Value ?? -1) == 0;
        }

    }
}
using Aplicacion.DTOs;
using Aplicacion.DTOs.ComunSPID;
using Aplicacion.DTOs.FXD_Local;
using Aplicacion.DTOs.SPID;
using Aplicacion.Interfaces.SPID;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.SPID
{
    /// <summary>
    /// Representa Comun SPIDDao.
    /// </summary>
    public class ComunSPIDDao : RepositorioMaestro, IComunSPIDDao
    {
        private const string SP_OBTENER_DATOS_FIRMA = "dbo.usp_LecturaSPID_ObtenerDatosFirma";
        private const string SP_OBTENER_DATOS_GENERACION_FIRMA = "dbo.usp_LecturaSPID_ObtenerDatosGeneracionFirma";
        private const string SP_ACTUALIZAR_FIRMA = "dbo.usp_LecturaSPID_ActualizarFirmaDigital";

        /// <summary>
        /// Inicializa una nueva instancia de la clase ComunSPIDDao.
        /// </summary>
        public ComunSPIDDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public async Task<SpidFirmaDigitalDto> ObtenerDatosFirma(string tabla, string folio)
        {
            var r = await EjecutarStoredProcedureQuery<SpidFirmaDigitalDto>(
                SP_OBTENER_DATOS_FIRMA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tabla", tabla));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

            return r.Data?.LastOrDefault();
        }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public async Task<SpidGeneraFirmaDto> ObtenerDatosGeneracionFirma(string tabla, string folio)
        {
            var r = await EjecutarStoredProcedureQuery<SpidGeneraFirmaDto>(
                SP_OBTENER_DATOS_GENERACION_FIRMA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tabla", tabla));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

            return r.Data?.LastOrDefault();
        }

        /// <summary>
        /// Ejecuta la operación public.
        /// </summary>
        public async Task<bool> ActualizarFirma(string tabla, string folio, string firma)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                SP_ACTUALIZAR_FIRMA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tabla", tabla));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@firma", firma));
                });

            return r.Exito;
        }

    }
}
using Aplicacion.DTOs.ENT_COAS;
using Aplicacion.Enums;
using Aplicacion.Interfaces.ENT_COAS;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace Infraestructura.Repositorio.ENT_COAS
{
    /// <summary>
    /// Representa ENTCOASSpei Dao.
    /// </summary>
    public class ENTCOASSpeiDao : RepositorioMaestro, IENTCOASSpeiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTCOASSpeiDao.
        /// </summary>
        public ENTCOASSpeiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI, null, null) { }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<List<EntCoasParticipanteSpeiDto>> ObtenerParticipantesSpei()
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasParticipanteSpeiDto>(
                ConstantesServicio.ProcedimientosENTCOAS.SP_INTERFACE_TOMI_SPEI_PARTICIPANTES,
                cmd =>
                {
                },
                MapearParticipanteSpei,
                CancellationToken.None);

            return r.Data?.ToList() ?? new List<EntCoasParticipanteSpeiDto>();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntCoasRespuestaSpeiDto> ResponderEntradaSpei(EntCoasResponderEntradaSpeiRequestDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasRespuestaSpeiDto>(
                ConstantesServicio.ProcedimientosENTCOAS.SP_INTERFACE_TOMI_SPEI_CAMBIA_ESTATUS_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", dto.OpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", dto.OpFechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio", dto.OpFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cve_rastreo", dto.OpCveRastreo, 30));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_monto", dto.OpMonto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", dto.CdClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estatus", dto.OpEstatus, 4));
                },
                MapearRespuestaSpei,
                CancellationToken.None);

            return r.Data?.FirstOrDefault();
        }

        private static EntCoasParticipanteSpeiDto MapearParticipanteSpei(DbDataReader reader)
        {
            return new EntCoasParticipanteSpeiDto
            {
                InsClave = ObtenerDecimal(reader, 0) ?? 0,
                InsIndice = ObtenerDecimal(reader, 1) ?? 0,
                InsNombre = ObtenerString(reader, 2),
                InsEdoRecep = ObtenerString(reader, 3),
                InsEdoConec = ObtenerString(reader, 4),
                InsEstado = ObtenerString(reader, 5)
            };
        }

        private static EntCoasRespuestaSpeiDto MapearRespuestaSpei(DbDataReader reader)
        {
            return new EntCoasRespuestaSpeiDto
            {
                OpError = ObtenerDecimal(reader, 0),
                OpClave = ObtenerDecimal(reader, 1),
                OpFechaOper = ObtenerDecimal(reader, 2),
                OpFolio = ObtenerDecimal(reader, 3),
                OpDescripcion = ObtenerString(reader, 4)
            };
        }

        private static decimal? ObtenerDecimal(DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
                return null;

            return Convert.ToDecimal(reader.GetValue(ordinal));
        }

        private static string ObtenerString(DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
                return null;

            return Convert.ToString(reader.GetValue(ordinal));
        }

    }
}

using Aplicacion.DTOs.ENT_COAS;
using Aplicacion.Enums;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.ENT_COAS
{
    /// <summary>
    /// Representa ENTCOASTomi Dao.
    /// </summary>
    public class ENTCOASTomiDao : RepositorioMaestro, IENTCOASTomiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTCOASTomiDao.
        /// </summary>
        public ENTCOASTomiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene datos recibidos COAS por folio y fecha sistema.
        /// </summary>
        public async Task<EntCoasOrdenPagoDto> ObtenerOrdenPagoRecibidaPorFolio(
            string folio,
            int fechaSistema)
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasOrdenPagoDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_OBTENER_ORDEN_PAGO_RECIBIDA_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", fechaSistema));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Marca una orden emitida como devuelta por datos de SPEI.
        /// </summary>
        public async Task<bool> MarcarOrdenEmitidaComoDevuelta(
            int fechaOper,
            int insClave,
            decimal importe,
            string cveRastreo,
            int causaDevolucion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ORDEN_EMITIDA_COMO_DEVUELTA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaOper", fechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@InsClave", insClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CveRastreo", cveRastreo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CausaDevolucion", causaDevolucion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene una orden emitida devuelta para validación de devolución.
        /// </summary>
        public async Task<EntCoasOrdenPagoDto> ObtenerOrdenEmitidaDevueltaParaValidacion(
            int fechaOper,
            int insClave,
            decimal importe,
            string cveRastreo)
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasOrdenPagoDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_OBTENER_ORDEN_EMITIDA_DEVUELTA_PARA_VALIDACION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaOper", fechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@InsClave", insClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CveRastreo", cveRastreo));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Cancela el plan de trabajo maestro como CANCELADA - DEVUELTA.
        /// </summary>
        public async Task<bool> CancelarPlanTrabajoMaestroDevuelto(
            string folio,
            string tipoOperacion,
            string razonRechazo,
            string fechaValor,
            string moneda,
            decimal importe)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_CANCELAR_PLAN_TRABAJO_MAESTRO_DEVUELTO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RazonRechazo", razonRechazo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca una orden recibida como devuelta por folio.
        /// </summary>
        public async Task<bool> MarcarOrdenRecibidaDevueltaPorFolio(
            string folio,
            int causaDevolucion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ORDEN_RECIBIDA_DEVUELTA_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CausaDevolucion", causaDevolucion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Inserta una devolución COAS desde la orden recibida.
        /// </summary>
        public async Task<bool> InsertarDevolucionCoasDesdeOrdenRecibida(
            string folio,
            int fechaSistema,
            int causaDevolucion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_INSERTAR_DEVOLUCION_COAS_DESDE_ORDEN_RECIBIDA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaSistema", fechaSistema));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CausaDevolucion", causaDevolucion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza los datos generales del plan de trabajo maestro.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroDatosGenerales(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string fechaValor,
            string moneda,
            decimal importe,
            string subtipoOperacion,
            int? numCliente,
            string nombreCliente,
            string numCuenta)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_DATOS_GENERALES,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", nombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", numCuenta));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cuenta las entradas SPEI existentes en TOMI por folio.
        /// </summary>
        public async Task<int> ContarEntradasSpeiPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_CONTAR_ENTRADAS_SPEI_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Marca una orden recibida COAS como aceptada.
        /// </summary>
        public async Task<bool> MarcarOrdenRecibidaAceptada(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ORDEN_RECIBIDA_ACEPTADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca una entrada SPEI como procesada.
        /// </summary>
        public async Task<bool> MarcarEntradaSpeiProcesada(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ENTRADA_SPEI_PROCESADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Finaliza el plan de trabajo maestro como RECIBIDA - NO ABONADA para el caso BBC.
        /// </summary>
        public async Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBbc(
            string folio,
            string tipoOperacion,
            string subtipoOperacion,
            string fechaValor,
            string moneda,
            decimal importe,
            string descripcionError)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA_BBC,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@DescripcionError", descripcionError));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Finaliza el detalle del plan de trabajo excepto el servicio CEP.
        /// </summary>
        public async Task<bool> FinalizarDetallePlanTrabajoExceptoCep(
            string folio,
            string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_FINALIZAR_DETALLE_PLAN_TRABAJO_EXCEPTO_CEP,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Finaliza el plan de trabajo maestro como RECIBIDA - NO ABONADA.
        /// </summary>
        public async Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonada(
            string folio,
            string tipoOperacion,
            string subtipoOperacion,
            string fechaValor,
            string moneda,
            decimal importe,
            string descripcionError)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@DescripcionError", descripcionError));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cuenta los registros de entrada/salida existentes por folio único.
        /// </summary>
        public async Task<int> ContarEntradasSalidasPorFolioUnico(string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_CONTAR_ENTRADAS_SALIDAS_POR_FOLIO_UNICO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro como RECIBIDA - NO ABONADA.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroRecibidaNoAbonada(
            string folio,
            string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca una orden recibida como rechazada con causa de devolución.
        /// </summary>
        public async Task<bool> MarcarOrdenRecibidaRechazada(
            string folio,
            int causaDevolucion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ORDEN_RECIBIDA_RECHAZADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CausaDevolucion", causaDevolucion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca una entrada SPEI como rechazada o cancelada.
        /// </summary>
        public async Task<bool> MarcarEntradaSpeiRechazada(
            string folio,
            int causaDevolucion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ENTRADA_SPEI_RECHAZADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CausaDevolucion", causaDevolucion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca una entrada SPEI como confirmada pendiente.
        /// </summary>
        public async Task<bool> MarcarEntradaSpeiConfirmadaPendiente(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_MARCAR_ENTRADA_SPEI_CONFIRMADA_PENDIENTE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene datos para conciliación TOMI por folio.
        /// </summary>
        public async Task<EntCoasConciliacionTomiDto> ObtenerConciliacionTomiPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasConciliacionTomiDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_OBTENER_CONCILIACION_TOMI_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene datos para conciliación de entrada/salida por referencia.
        /// </summary>
        public async Task<EntCoasConciliacionEntradaSalidaDto> ObtenerConciliacionEntradaSalidaPorReferencia(string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasConciliacionEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                });

            return r.Data?.FirstOrDefault();
        }


        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoSpei(EntCoasInsertarFormatoSpeiDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_TOMBIN_INSERT_FORMATO_SPEI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", dto.OpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", dto.OpFechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@me_clave", dto.MeClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio", dto.OpFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ins_clave", dto.InsClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_monto", dto.OpMonto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tp_clave", dto.TpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cve_rastreo", dto.OpCveRastreo, 30));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ord", dto.OpNomOrd, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ord", dto.TcClaveOrd));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ord", dto.OpCuentaOrd, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ord", dto.OpRfcCurpOrd, 18));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ben", dto.OpNomBen, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ben", dto.TcClaveBen));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ben", dto.OpCuentaBen, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ben", dto.OpRfcCurpBen, 18));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ben2", dto.OpNomBen2, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ben2", dto.TcClaveBen2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ben2", dto.OpCuentaBen2, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ben2", dto.OpRfcCurpBen2, 18));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_concepto_pago", dto.OpConceptoPago, 210));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_concepto_pag2", dto.OpConceptoPag2, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_iva", dto.OpIva));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_ref_numerica", dto.OpRefNumerica));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_ref_cobranza", dto.OpRefCobranza, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave_pago", dto.OpClavePago, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@to_clave", dto.ToClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", dto.CdClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estado", dto.OpEstado, 2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cde", dto.OpCde, 255));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_cap", dto.OpFechaCap));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_cap", dto.OpHoraCap));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_tipo_orden", dto.OpTipoOrden, 1));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_prioridad", dto.OpPrioridad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@usu_clave", dto.UsuClave, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio_servidor", dto.OpFolioServidor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_razon_rechazo", dto.OpRazonRechazo, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_usu_autoriza", dto.OpUsuAutoriza, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_liq_bm", dto.OpHoraLiqBm));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_liq_sist", dto.OpHoraLiqSist));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_Estatus", dto.NfEstatus, 4));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", dto.NfFolioUnico, 25));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", dto.T24CustomerCode, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_COMPANY_NAME", dto.T24CompanyName, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", dto.T24BenAcctNo, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", dto.NfBancoBenef, 60));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", dto.NfFechaEjecucion));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && Convert.ToInt32(salidaParam?.Value ?? -1) == 0;
        }

        /// <summary>
        /// Obtiene datos base para construir la entrada/salida.
        /// </summary>
        public async Task<EntCoasEntradaSpeiTomiDto> ObtenerEntradaSpeiTomiPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntCoasEntradaSpeiTomiDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_LECTURA_ENT_COAS_OBTENER_ENTRADA_SPEI_TOMI_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalida(EntCoasInsertarEntradaSalidaDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTCOAS.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", dto.Referencia, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", dto.NumCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", dto.Cdl));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", dto.Ordenante, 255));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", dto.Operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", dto.OperacionDesc, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", dto.Tipo, 1));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", dto.TipoDesc, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", dto.Cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", dto.Nombre, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", dto.CuentaMoneda, 3));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", dto.Via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", dto.ViaDesc, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", dto.Saldo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", dto.Comentarios, 96));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", dto.FechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", dto.FechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", dto.Hora, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", dto.Capturista, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", dto.Fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", dto.ReferenciaControl, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", dto.ReferenciaControlCont));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && Convert.ToInt32(salidaParam?.Value ?? -1) == 0;
        }

    }
}
using Aplicacion.DTOs.ENT_SPEI;
using Aplicacion.Enums;
using Aplicacion.Interfaces.SPEI;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Infraestructura.Repositorio;

namespace Infraestructura.Repositorio.SPEI
{
    /// <summary>
    /// Representa Ent Spei Dao.
    /// </summary>
    public class EntSpeiDao : RepositorioMaestro, IEntSpeiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase EntSpeiDao.
        /// </summary>
        public EntSpeiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene la entrada SPEI NF por folio.
        /// </summary>
        public async Task<EntSpeiNfEntradaSpeiDto> ObtenerNfEntradaSpeiPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiNfEntradaSpeiDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_OBTENER_NF_ENTRADA_SPEI_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza los datos generales del plan de trabajo maestro.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroDatosGral(
            string folio,
            string tipoOperacion,
            string folioUnico,
            int fechaValor,
            string moneda,
            decimal importe,
            string subtipoOperacion,
            bool incluirDatosCliente,
            string numCliente,
            string nombreCliente,
            string numCuenta)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_DATOS_GRAL,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IncluirDatosCliente", incluirDatosCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", nombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", numCuenta));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoSpei(
            long opClave,
            int opFechaOper,
            int meClave,
            int opFolio,
            long insClave,
            decimal opMonto,
            int tpClave,
            string opCveRastreo,
            string opNomOrd,
            int tcClaveOrd,
            string opCuentaOrd,
            string opRfcCurpOrd,
            string opNomBen,
            int? tcClaveBen,
            string opCuentaBen,
            string opRfcCurpBen,
            string opNomBen2,
            int? tcClaveBen2,
            string opCuentaBen2,
            string opRfcCurpBen2,
            string opConceptoPago,
            string opConceptoPag2,
            decimal? opIva,
            int? opRefNumerica,
            string opRefCobranza,
            string opClavePago,
            int? toClave,
            int? cdClave,
            string opEstado,
            string opCde,
            int? opFechaCap,
            int? opHoraCap,
            string opTipoOrden,
            int? opPrioridad,
            string usuClave,
            long? opFolioServidor,
            string opRazonRechazo,
            string opUsuAutoriza,
            int? opHoraLiqBm,
            long? opHoraLiqSist,
            string nfEstatus,
            string nfFolioUnico,
            string t24CustomerCode,
            string t24CompanyName,
            string t24BenAcctNo,
            string nfBancoBenef,
            int nfFechaEjecucion)
        {
            var salida = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_TOMBIN_INSERT_FORMATO_SPEI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", opClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", opFechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@me_clave", meClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio", opFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ins_clave", insClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_monto", opMonto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tp_clave", tpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cve_rastreo", opCveRastreo, 30));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ord", opNomOrd, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ord", tcClaveOrd));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ord", opCuentaOrd, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ord", opRfcCurpOrd, 18));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ben", opNomBen, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ben", tcClaveBen));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ben", opCuentaBen, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ben", opRfcCurpBen, 18));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ben2", opNomBen2, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ben2", tcClaveBen2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ben2", opCuentaBen2, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ben2", opRfcCurpBen2, 18));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_concepto_pago", opConceptoPago, 210));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_concepto_pag2", opConceptoPag2, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_iva", opIva));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_ref_numerica", opRefNumerica));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_ref_cobranza", opRefCobranza, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave_pago", opClavePago, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@to_clave", toClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", cdClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estado", opEstado, 2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cde", opCde, 255));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_cap", opFechaCap));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_cap", opHoraCap));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_tipo_orden", opTipoOrden, 1));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_prioridad", opPrioridad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@usu_clave", usuClave, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio_servidor", opFolioServidor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_razon_rechazo", opRazonRechazo, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_usu_autoriza", opUsuAutoriza, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_liq_bm", opHoraLiqBm));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_liq_sist", opHoraLiqSist));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_Estatus", nfEstatus, 4));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", nfFolioUnico, 25));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", t24CustomerCode, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_COMPANY_NAME", t24CompanyName, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", t24BenAcctNo, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", nfBancoBenef, 60));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", nfFechaEjecucion));
                    cmd.Parameters.Add(salida);
                });

            return r.Exito && OutputParameterHelper.EsSalidaCero(salida, ConstantesServicio.ProcedimientosENTSPEI.USP_TOMBIN_INSERT_FORMATO_SPEI);
        }

        /// <summary>
        /// Cuenta los registros NF de entradas SPEI por folio.
        /// </summary>
        public async Task<int> ContarNfEntradasSpeiPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_CONTAR_NF_ENTRADAS_SPEI_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Marca la entrada SPEI NF como procesada.
        /// </summary>
        public async Task<bool> MarcarNfEntradaSpeiComoProcesada(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_MARCAR_NF_ENTRADA_SPEI_COMO_PROCESADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo como RECIBIDA - NO ABONADA.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoRecibidaNoAbonada(
            string folio,
            string tipoOperacion,
            string subtipoOperacion,
            string descripcionError,
            int fechaValor,
            string moneda,
            decimal importe,
            bool flgTerminado)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_TRABAJO_RECIBIDA_NO_ABONADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@DescripcionError", descripcionError));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FlgTerminado", flgTerminado));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cierra el detalle del plan de trabajo excepto CEP.
        /// </summary>
        public async Task<bool> CerrarPlanTrabajoDetalleExceptoCep(string folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_CERRAR_PLAN_TRABAJO_DETALLE_EXCEPTO_CEP,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalidaSrv(
            string referencia,
            int numCliente,
            double cdl,
            string ordenante,
            double operacion,
            string operacionDesc,
            string tipo,
            string tipoDesc,
            double cuenta,
            string nombre,
            string cuentaMoneda,
            double via,
            string viaDesc,
            decimal importe,
            decimal saldo,
            string comentarios,
            DateTime fechaCaptura,
            DateTime fechaValor,
            string hora,
            string capturista,
            double fad,
            string referenciaControl,
            double referenciaControlCont)
        {
            var salida = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", cdl));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", ordenante, 255));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", operacionDesc, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", tipo, 1));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", tipoDesc, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", nombre, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", cuentaMoneda, 3));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", viaDesc, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", saldo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", comentarios, 96));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", fechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", hora, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", capturista, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", referenciaControl, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", referenciaControlCont));
                    cmd.Parameters.Add(salida);
                });

            return r.Exito && Convert.ToInt32(salida.Value) == 0;
        }

        /// <summary>
        /// Cuenta las entradas/salidas asociadas a la referencia.
        /// </summary>
        public async Task<int> ContarEntradasSalidasPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Actualiza el plan de trabajo por rechazo sin abono.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoRechazoSinAbono(
            string folio,
            string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_TRABAJO_RECHAZO_SIN_ABONO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca la entrada SPEI NF como CONP.
        /// </summary>
        public async Task<bool> MarcarNfEntradaSpeiComoConp(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_MARCAR_NF_ENTRADA_SPEI_COMO_CONP,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene los datos de conciliación TOMI por folio.
        /// </summary>
        public async Task<EntSpeiTomiConciliacionDto> ObtenerConciliacionTomiPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiTomiConciliacionDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_OBTENER_CONCILIACION_TOMI_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene los datos de conciliación de entrada/salida por referencia.
        /// </summary>
        public async Task<EntSpeiEntradaSalidaConciliacionDto> ObtenerConciliacionEntradaSalidaPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiEntradaSalidaConciliacionDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene el catálogo de devoluciones automáticas.
        /// </summary>
        public async Task<EntSpeiAutoDevolucionDto[]> ObtenerCatalogoDevolucionesAutomaticas()
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiAutoDevolucionDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_OBTENER_CATALOGO_DEVOLUCIONES_AUTOMATICAS,
                cmd => { });

            return r.Data?.ToArray() ?? Array.Empty<EntSpeiAutoDevolucionDto>();
        }

        /// <summary>
        /// Obtiene la cuenta Vostro por clave Swift o cuenta beneficiario.
        /// </summary>
        public async Task<string> ObtenerCuentaVostro(string claveSwift, string cuentaBeneficiario)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiCuentaVostroDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_OBTENER_CUENTA_VOSTRO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ClaveSwift", claveSwift, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaBeneficiario", cuentaBeneficiario, 50));
                });

            return r.Data?.FirstOrDefault()?.CuentaBeneficiario ?? "0";
        }

        /// <summary>
        /// Actualiza el plan de trabajo por devolución.
        /// </summary>
        public async Task<bool> ActualizarPlanDevolucion(
            string folio,
            string tipoOperacion,
            string razonRechazo,
            int fechaEjecucion,
            string moneda,
            decimal importe)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.USP_LECTURA_ENT_SPEI_ACTUALIZAR_PLAN_DEVOLUCION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RazonRechazo", razonRechazo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaEjecucion", fechaEjecucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs;
using Aplicacion.DTOs.ENT_SPEI;
using Aplicacion.Enums;
using Aplicacion.Interfaces.SPEI;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.SPEI
{
    /// <summary>
    /// Representa Ent Spei Data Devolucion Raw Dto.
    /// </summary>
    internal class EntSpeiDataDevolucionRawDto
    {
        public decimal? op_fecha_oper { get; set; }
        public string op_cve_rastreo { get; set; }
        public decimal? ins_clave { get; set; }
        public decimal? op_monto { get; set; }
    }
/// <summary>
/// Representa Ent Spei Spei Dao.
/// </summary>

    public class EntSpeiSpeiDao : RepositorioMaestro, IEntSpeiSpeiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase EntSpeiSpeiDao.
        /// </summary>
        public EntSpeiSpeiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPEI1, null, null) { }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntSpeiParticipanteDto[]> ObtenerParticipantes()
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiParticipanteDto>(
                ConstantesServicio.ProcedimientosENTSPEI.SP_INTERFACE_TOMI_SPEI_PARTICIPANTES,
                cmd => { });

            return r.Data?.ToArray() ?? new EntSpeiParticipanteDto[0];
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntSpeiOperacionRecibidaDto[]> ObtenerOperacionesRecibidasPorFecha(decimal fechaOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiOperacionRecibidaDto>(
                ConstantesServicio.ProcedimientosENTSPEI.SP_INTERFACE_TOMI_SPEI_ESTATUS_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", fechaOperacion));
                });

            return r.Data?.ToArray() ?? Array.Empty<EntSpeiOperacionRecibidaDto>();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntSpeiCambioEstatusRespuestaDto> CambiarEstatusRecibida(
            decimal opClave,
            decimal opFechaOper,
            decimal opFolio,
            string opCveRastreo,
            decimal opMonto,
            decimal? cdClave,
            string opEstatus)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpeiCambioEstatusRespuestaDto>(
                ConstantesServicio.ProcedimientosENTSPEI.SP_INTERFACE_TOMI_SPEI_CAMBIA_ESTATUS_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", opClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", opFechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio", opFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cve_rastreo", opCveRastreo, 30));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_monto", opMonto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", cdClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estatus", opEstatus, 4));
                },
                MapearCambioEstatusRecibida,
                CancellationToken.None);

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntSpeiDataDevolucionDto> ObtenerDataDevolucionPorFolio(string folio)
        {
            decimal folioNumerico = decimal.Parse(folio, CultureInfo.InvariantCulture);

            var r = await EjecutarStoredProcedureQuery<EntSpeiDataDevolucionRawDto>(
                ConstantesServicio.ProcedimientosENTSPEI.SP_INTERFACE_TOMI_GET_DATA_DEV,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", folioNumerico));
                });

            var data = r.Data?.FirstOrDefault();
            if (data == null)
                return null;

            return new EntSpeiDataDevolucionDto
            {
                OpFechaOper = data.op_fecha_oper.HasValue ? (int?)Convert.ToInt32(data.op_fecha_oper.Value) : null,
                OpCveRastreo = data.op_cve_rastreo,
                InsClave = data.ins_clave.HasValue ? (int?)Convert.ToInt32(data.ins_clave.Value) : null,
                OpMonto = data.op_monto.HasValue ? (double?)Convert.ToDouble(data.op_monto.Value) : null
            };
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> EjecutarDevolucion(string folio, int causaDevolucion, string firmaDigital)
        {
            decimal folioNumerico = decimal.Parse(folio, CultureInfo.InvariantCulture);

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPEI.SP_INTERFACE_TOMI_SPEI_0,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", folioNumerico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", causaDevolucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_firma_dig", firmaDigital));
                });

            return r.Exito;
        }

        private static EntSpeiCambioEstatusRespuestaDto MapearCambioEstatusRecibida(DbDataReader reader)
        {
            return new EntSpeiCambioEstatusRespuestaDto
            {
                op_error = ObtenerDecimal(reader, 0),
                op_clave = ObtenerDecimal(reader, 1),
                op_fecha_oper = ObtenerDecimal(reader, 2),
                op_folio = ObtenerDecimal(reader, 3),
                op_descripcion = ObtenerString(reader, 4)
            };
        }

        private static decimal? ObtenerDecimal(DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
                return null;

            return Convert.ToDecimal(reader.GetValue(ordinal));
        }

        private static string ObtenerString(DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
                return null;

            return Convert.ToString(reader.GetValue(ordinal));
        }
    }
}
using Aplicacion.DTOs.ENT_SPID;
using Aplicacion.Enums;
using Aplicacion.Interfaces.ENT_SPID;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.ENT_SPID
{
    /// <summary>
    /// Representa ENTSPIDSpid Dao.
    /// </summary>
    public class ENTSPIDSpidDao : RepositorioMaestro, IENTSPIDSpidDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTSPIDSpidDao.
        /// </summary>
        public ENTSPIDSpidDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.SPID, null, null) { }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<List<EntSpidParticipanteDto>> ObtenerParticipantesSpid()
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidParticipanteDto>(
                ConstantesServicio.ProcedimientosENTSPID.SP_INTERFACE_TOMI_SPID_PARTICIPANTES,
                cmd =>
                {
                },
                MapearParticipanteSpid,
                CancellationToken.None);

            return r.Data?.ToList() ?? new List<EntSpidParticipanteDto>();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<List<EntSpidOperacionRecibidaDto>> ObtenerOperacionesRecibidasPorFechaOperacion(decimal fechaOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidOperacionRecibidaDto>(
                ConstantesServicio.ProcedimientosENTSPID.SP_INTERFACE_TOMI_SPID_ESTATUS_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", fechaOperacion));
                },
                MapearOperacionRecibida,
                CancellationToken.None);

            return r.Data?.ToList() ?? new List<EntSpidOperacionRecibidaDto>();
        }
        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> CambiarEstatusRecibidaSpid(EntSpidCambiarEstatusRecibidaDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidCambioEstatusResultadoDto>(
                ConstantesServicio.ProcedimientosENTSPID.SP_INTERFACE_TOMI_SPID_CAMBIA_ESTATUS_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", dto.OpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", dto.OpFechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio", dto.OpFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cve_rastreo", dto.OpCveRastreo, 30));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_monto", dto.OpMonto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", dto.CausaDevolucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estatus", dto.OpEstatus, 4));
                },
                MapearCambioEstatus,
                CancellationToken.None);

            return r.Data?.FirstOrDefault()?.Codigo == 0;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntSpidDatosDevolucionDto> ObtenerDatosDevolucionSpid(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidDatosDevolucionDto>(
                ConstantesServicio.ProcedimientosENTSPID.SP_INTERFACE_TOMI_GET_DATA_DEV,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", folio));
                },
                MapearDatosDevolucion,
                CancellationToken.None);

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> DevolverEntradaSpid(EntSpidDevolverEntradaDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.SP_INTERFACE_TOMI_SPID_0,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", dto.OpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", dto.CausaDevolucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_firma_dig", dto.OpFirmaDigital));
                });

            return r.Exito;
        }

        private static EntSpidParticipanteDto MapearParticipanteSpid(DbDataReader reader)
        {
            return new EntSpidParticipanteDto
            {
                InsClave = ObtenerDecimal(reader, "ins_clave", 0) ?? 0,
                InsNombre = ObtenerString(reader, "ins_nombre", 2)
            };
        }

        private static EntSpidOperacionRecibidaDto MapearOperacionRecibida(DbDataReader reader)
        {
            return new EntSpidOperacionRecibidaDto
            {
                OpClave = ObtenerDecimal(reader, "op_clave", 1) ?? 0,
                OpFechaOper = ObtenerDecimal(reader, "op_fecha_oper", 2) ?? 0,
                MeClave = ObtenerDecimal(reader, "me_clave", 3) ?? 0,
                OpFolio = ObtenerDecimal(reader, "op_folio", 4) ?? 0,
                InsClave = ObtenerDecimal(reader, "ins_clave", 5) ?? 0,
                OpMonto = ObtenerDecimal(reader, "op_monto", 6) ?? 0,
                TpClave = ObtenerDecimal(reader, "tp_clave", 7) ?? 0,
                OpCveRastreo = ObtenerString(reader, "op_cve_rastreo", 8),
                OpNomOrd = ObtenerString(reader, "op_nom_ord", 9),
                TcClaveOrd = ObtenerDecimal(reader, "tc_clave_ord", 10),
                OpCuentaOrd = ObtenerString(reader, "op_cuenta_ord", 11),
                OpRfcCurpOrd = ObtenerString(reader, "op_rfc_curp_ord", 12),
                OpNomBen = ObtenerString(reader, "op_nom_ben", 13),
                TcClaveBen = ObtenerDecimal(reader, "tc_clave_ben", 14),
                OpCuentaBen = ObtenerString(reader, "op_cuenta_ben", 15),
                OpRfcCurpBen = ObtenerString(reader, "op_rfc_curp_ben", 16),
                OpNomBen2 = ObtenerString(reader, "op_nom_ben2", 17),
                TcClaveBen2 = ObtenerDecimal(reader, "tc_clave_ben2", 18),
                OpCuentaBen2 = ObtenerString(reader, "op_cuenta_ben2", 19),
                OpRfcCurpBen2 = ObtenerString(reader, "op_rfc_curp_ben2", 20),
                OpConceptoPago = ObtenerString(reader, "op_concepto_pago", 21),
                OpConceptoPag2 = ObtenerString(reader, "op_concepto_pag2", 22),
                OpIva = ObtenerDecimal(reader, "op_iva", 23),
                OpRefNumerica = ObtenerString(reader, "op_ref_numerica", 24),
                OpRefCobranza = ObtenerString(reader, "op_ref_cobranza", 25),
                OpClavePago = ObtenerString(reader, "op_clave_pago", 26),
                ToClave = ObtenerDecimal(reader, "to_clave", 27),
                CdClave = ObtenerDecimal(reader, "cd_clave", 28),
                OpEstado = ObtenerString(reader, "op_estado", 29),
                OpCde = ObtenerString(reader, "op_cde", 30),
                OpFechaCap = ObtenerDecimal(reader, "op_fecha_cap", 31) ?? 0,
                OpHoraCap = ObtenerDecimal(reader, "op_hora_cap", 32) ?? 0,
                OpTipoOrden = ObtenerString(reader, "op_tipo_orden", 33),
                OpPrioridad = ObtenerDecimal(reader, "op_prioridad", 34) ?? 0,
                UsuClave = ObtenerString(reader, "usu_clave", 35),
                OpFolioServidor = ObtenerDecimal(reader, "op_folio_servidor", 36),
                OpRazonRechazo = ObtenerString(reader, "op_razon_rechazo", 37),
                OpUsuAutoriza = ObtenerString(reader, "op_usu_autoriza", 38),
                OpHoraLiqBm = ObtenerDecimal(reader, "op_hora_liq_bm", 45),
                OpHoraLiqSist = ObtenerDecimal(reader, "op_hora_liq_sist", 46),
                OpFirmaDig = ObtenerString(reader, "OP_FIRMA_DIG", 70)
            };
        }

        private static EntSpidCambioEstatusResultadoDto MapearCambioEstatus(DbDataReader reader)
        {
            return new EntSpidCambioEstatusResultadoDto
            {
                Codigo = ObtenerInt32(reader, null, 0) ?? 1,
                Mensaje = ObtenerString(reader, null, 1)
            };
        }

        private static EntSpidDatosDevolucionDto MapearDatosDevolucion(DbDataReader reader)
        {
            return new EntSpidDatosDevolucionDto
            {
                OpFechaOper = ObtenerInt32(reader, "op_fecha_oper", 0) ?? 0,
                OpCveRastreo = ObtenerString(reader, "op_cve_rastreo", 1),
                InsClave = Convert.ToInt64(ObtenerDecimal(reader, "ins_clave", 2) ?? 0),
                OpMonto = ObtenerDecimal(reader, "op_monto", 3) ?? 0
            };
        }

        private static decimal? ObtenerDecimal(DbDataReader reader, string nombreColumna, int ordinalFallback)
        {
            int ordinal = ObtenerOrdinal(reader, nombreColumna, ordinalFallback);
            if (ordinal < 0 || reader.IsDBNull(ordinal))
                return null;

            return Convert.ToDecimal(reader.GetValue(ordinal));
        }

        private static int? ObtenerInt32(DbDataReader reader, string nombreColumna, int ordinalFallback)
        {
            decimal? valor = ObtenerDecimal(reader, nombreColumna, ordinalFallback);
            return valor.HasValue ? (int?)Convert.ToInt32(valor.Value) : null;
        }

        private static string ObtenerString(DbDataReader reader, string nombreColumna, int ordinalFallback)
        {
            int ordinal = ObtenerOrdinal(reader, nombreColumna, ordinalFallback);
            if (ordinal < 0 || reader.IsDBNull(ordinal))
                return null;

            return Convert.ToString(reader.GetValue(ordinal));
        }

        private static int ObtenerOrdinal(DbDataReader reader, string nombreColumna, int ordinalFallback)
        {
            if (!string.IsNullOrEmpty(nombreColumna))
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (string.Equals(reader.GetName(i), nombreColumna, StringComparison.OrdinalIgnoreCase))
                        return i;
                }
            }

            return ordinalFallback >= 0 && ordinalFallback < reader.FieldCount ? ordinalFallback : -1;
        }
    }
}
using Aplicacion.DTOs.ENT_SPID;
using Aplicacion.Enums;
using Aplicacion.Interfaces.ENT_SPID;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.ENT_SPID
{
    /// <summary>
    /// Representa ENTSPIDTomi Dao.
    /// </summary>
    public class ENTSPIDTomiDao : RepositorioMaestro, IENTSPIDTomiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTSPIDTomiDao.
        /// </summary>
        public ENTSPIDTomiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Actualiza datos del plan de trabajo maestro a estado LECTURA.
        /// </summary>
        public async Task<bool> ActualizarDatosPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            decimal fechaValor,
            string moneda,
            decimal importe,
            string subtipoOperacion,
            bool actualizarDatosCliente,
            int? numCliente,
            string nombreCliente,
            string numCuenta)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_ACTUALIZAR_DATOS_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ActualizarDatosCliente", actualizarDatosCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", nombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", numCuenta));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cuenta las entradas SPID por folio.
        /// </summary>
        public async Task<int> ContarEntradasSpidPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_CONTAR_ENTRADAS_SPID_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Marca la entrada SPID como procesada.
        /// </summary>
        public async Task<bool> MarcarEntradaSpidProcesada(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_MARCAR_ENTRADA_SPID_PROCESADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Finaliza el plan de trabajo maestro como RECIBIDA - NO ABONADA para BTMUM.
        /// </summary>
        public async Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonadaBTMUM(
            string folio,
            string tipoOperacion,
            string subtipoOperacion,
            decimal fechaValor,
            string moneda,
            decimal importe,
            string descripcionError)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA_BTMUM,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@DescripcionError", descripcionError));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cuenta las entradas/salidas por referencia.
        /// </summary>
        public async Task<int> ContarEntradasSalidasPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Finaliza el plan de trabajo maestro como RECIBIDA - NO ABONADA.
        /// </summary>
        public async Task<bool> FinalizarPlanTrabajoMaestroRecibidaNoAbonada(
            string folio,
            string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_FINALIZAR_PLAN_TRABAJO_MAESTRO_RECIBIDA_NO_ABONADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cancela el plan de trabajo maestro por devolución.
        /// </summary>
        public async Task<bool> CancelarPlanTrabajoMaestroPorDevolucion(
            string folio,
            string tipoOperacion,
            string razonRechazo,
            decimal fechaValor,
            string moneda,
            decimal importe)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_CANCELAR_PLAN_TRABAJO_MAESTRO_POR_DEVOLUCION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RazonRechazo", razonRechazo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca la entrada SPID como confirmada pendiente.
        /// </summary>
        public async Task<bool> MarcarEntradaSpidConfirmadaPendiente(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_MARCAR_ENTRADA_SPID_CONFIRMADA_PENDIENTE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene datos de conciliación de entrada SPID por folio.
        /// </summary>
        public async Task<EntSpidConciliacionTomiDto> ObtenerConciliacionEntradaSpidPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidConciliacionTomiDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_OBTENER_CONCILIACION_ENTRADA_SPID_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene datos de conciliación de entrada/salida por referencia.
        /// </summary>
        public async Task<EntSpidConciliacionEntradaSalidaDto> ObtenerConciliacionEntradaSalidaPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidConciliacionEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_OBTENER_CONCILIACION_ENTRADA_SALIDA_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene datos de entrada SPID para construir entrada/salida.
        /// </summary>
        public async Task<EntSpidEntradaSpidParaEntradaSalidaDto> ObtenerEntradaSpidParaEntradaSalida(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<EntSpidEntradaSpidParaEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_LECTURA_ENT_SPID_OBTENER_ENTRADA_SPID_PARA_ENTRADA_SALIDA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoSpidEnTomi(EntSpidInsertarFormatoTomiDto dto)
        {
            var salida = basedatosConexion.CrearParametroSalida("@Salida", System.Data.DbType.Int32);

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_TOMBIN_INSERT_FORMATO_SPID,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave", dto.OpClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", dto.OpFechaOper));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@me_clave", dto.MeClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio", dto.OpFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ins_clave", dto.InsClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_monto", dto.OpMonto));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tp_clave", dto.TpClave));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cve_rastreo", dto.OpCveRastreo, 30));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ord", dto.OpNomOrd, 120));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ord", dto.TcClaveOrd));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ord", dto.OpCuentaOrd, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ord", dto.OpRfcCurpOrd, 18));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ben", dto.OpNomBen, 120));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ben", dto.TcClaveBen));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ben", dto.OpCuentaBen, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ben", dto.OpRfcCurpBen, 18));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_nom_ben2", dto.OpNomBen2, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@tc_clave_ben2", dto.TcClaveBen2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cuenta_ben2", dto.OpCuentaBen2, 35));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_rfc_curp_ben2", dto.OpRfcCurpBen2, 18));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_concepto_pago", dto.OpConceptoPago, 210));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_concepto_pag2", dto.OpConceptoPag2, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_iva", dto.OpIva));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_ref_numerica", dto.OpRefNumerica, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_ref_cobranza", dto.OpRefCobranza, 40));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_clave_pago", dto.OpClavePago, 10));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@to_clave", dto.ToClave));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@cd_clave", dto.CdClave));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estado", dto.OpEstado, 2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_cde", dto.OpCde, 255));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_cap", dto.OpFechaCap));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_cap", dto.OpHoraCap));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_tipo_orden", dto.OpTipoOrden, 1));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_prioridad", dto.OpPrioridad));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@usu_clave", dto.UsuClave, 10));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_folio_servidor", dto.OpFolioServidor));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_razon_rechazo", dto.OpRazonRechazo, 90));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_usu_autoriza", dto.OpUsuAutoriza, 10));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_liq_bm", dto.OpHoraLiqBm));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_hora_liq_sist", dto.OpHoraLiqSist));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", dto.NfEstatus, 5));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", dto.NfFolioUnico, 65));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", dto.T24CustomerCode, 20));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_COMPANY_NAME", dto.T24CompanyName, 120));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", dto.T24BenAcctNo, 15));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", dto.NfBancoBenef, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", dto.NfFechaEjecucion));
                    cmd.Parameters.Add(salida);
                });

            return r.Exito && Convert.ToInt32(salida.Value) == 0;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalida(EntSpidInsertarEntradaSalidaDto dto)
        {
            var salida = basedatosConexion.CrearParametroSalida("@Salida", System.Data.DbType.Int32);

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTSPID.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", dto.Referencia, 50));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", dto.NumCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", dto.Cdl));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", dto.Ordenante, 255));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", dto.Operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", dto.OperacionDesc, 20));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", dto.Tipo, 1));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", dto.TipoDesc, 20));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", dto.Cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", dto.Nombre, 100));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", dto.CuentaMoneda, 3));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", dto.Via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", dto.ViaDesc, 100));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", dto.Saldo));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", dto.Comentarios, 96));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", dto.FechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", dto.FechaValor));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", dto.Hora, 15));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", dto.Capturista, 15));

                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", dto.Fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", dto.ReferenciaControl, 2));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", dto.ReferenciaControlCont));
                    cmd.Parameters.Add(salida);
                });

            return r.Exito && Convert.ToInt32(salida.Value) == 0;
        }

    }
}
using Aplicacion.DTOs.ENT_TEF;
using Aplicacion.Enums;
using Aplicacion.Interfaces.ENT_TEF;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.ENT_TEF
{
    /// <summary>
    /// Representa ENTTEFTef Dao.
    /// </summary>
    public class ENTTEFTefDao : RepositorioMaestro, IENTTEFTefDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTTEFTefDao.
        /// </summary>
        public ENTTEFTefDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TEF, null, null) { }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<List<EntTefParticipanteDto>> ObtenerParticipantes()
        {
            var r = await EjecutarStoredProcedureQuery<EntTefParticipanteDto>(
                ConstantesServicio.ProcedimientosENTTEF.SP_INTERFACE_TOMI_TEF_PARTICIPANTES,
                cmd =>
                {
                });

            return r.Data?.ToList() ?? new List<EntTefParticipanteDto>();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<List<EntTefRecibidaDto>> ObtenerRecibidasPorFechaAplicacion(int fechaAplicacion)
        {
            var r = await EjecutarStoredProcedureQuery<EntTefRecibidaDto>(
                ConstantesServicio.ProcedimientosENTTEF.SP_INTERFACE_TOMI_TEF_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", fechaAplicacion));
                });

            return r.Data?.ToList() ?? new List<EntTefRecibidaDto>();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntTefRecibidaDto> ObtenerRecibidaPorFolio(int fechaAplicacion, long idOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<EntTefRecibidaDto>(
                ConstantesServicio.ProcedimientosENTTEF.SP_INTERFACE_TOMI_TEF_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", fechaAplicacion));
                });

            return r.Data?.FirstOrDefault(x => x.IDOperacion == idOperacion);
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<EntTefCambiarEstatusResultadoDto> CambiarEstatusRecibida(long idMaster, int fechaProceso, string razonRechazo, string estatus)
        {
            var r = await EjecutarStoredProcedureQuery<EntTefCambiarEstatusResultadoDto>(
                ConstantesServicio.ProcedimientosENTTEF.SP_INTERFACE_TOMI_TEF_CAMBIA_ESTATUS_RECIBIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@id_master", idMaster));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_fecha_oper", fechaProceso));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_razon_rechazo", razonRechazo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@op_estatus", estatus));
                },
                MapearCambioEstatusRecibida,
                CancellationToken.None);

            return r.Data?.FirstOrDefault();
        }

        private static EntTefCambiarEstatusResultadoDto MapearCambioEstatusRecibida(DbDataReader reader)
        {
            return new EntTefCambiarEstatusResultadoDto
            {
                Codigo = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
                Mensaje = reader.IsDBNull(1) ? null : Convert.ToString(reader.GetValue(1))
            };
        }
    }
}
using Aplicacion.DTOs.ENT_TEF;
using Aplicacion.Enums;
using Aplicacion.Interfaces.ENT_TEF;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.ENT_TEF
{
    /// <summary>
    /// Representa ENTTEFTomi Dao.
    /// </summary>
    public class ENTTEFTomiDao : RepositorioMaestro, IENTTEFTomiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase ENTTEFTomiDao.
        /// </summary>
        public ENTTEFTomiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Actualiza los datos generales del maestro cuando la cuenta T24 fue encontrada, es válida y RFC/CURP válido.
        /// </summary>
        public async Task<bool> ActualizarDatosGeneralesMaestroConCliente(
            long folio,
            string tipoOperacion,
            string folioUnico,
            int fechaValor,
            string moneda,
            decimal importe,
            string subtipoOperacion,
            int numCliente,
            string nombreCliente,
            string numCuenta)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_ACTUALIZAR_DATOS_GENERALES_MAESTRO_CON_CLIENTE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", nombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", numCuenta));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza los datos generales del maestro sin datos de cliente T24.
        /// </summary>
        public async Task<bool> ActualizarDatosGeneralesMaestroSinCliente(
            long folio,
            string tipoOperacion,
            string folioUnico,
            int fechaValor,
            string moneda,
            decimal importe,
            string subtipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_ACTUALIZAR_DATOS_GENERALES_MAESTRO_SIN_CLIENTE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoEntradaTef(EntTefInsertarFormatoTomiDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_TOMBIN_INSERT_FORMATO_ENTRADA_TEF,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IDOperacion", dto.IDOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaProceso", dto.FechaProceso));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@BancoPresentador", dto.BancoPresentador));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@BancoReceptor", dto.BancoReceptor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Divisa", dto.Divisa));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaAplicacion", dto.FechaAplicacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaInicial", dto.FechaInicial));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ClaveRastreo", dto.ClaveRastreo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CodigoOperacion", dto.CodigoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", dto.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoCuentaOrdenante", dto.TipoCuentaOrdenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaOrdenante", dto.CuentaOrdenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreOrdenante", dto.NombreOrdenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RFCOrdenante", dto.RFCOrdenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RefNumOrdenante", dto.RefNumOrdenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RefLeyendaOrdenante", dto.RefLeyendaOrdenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoCuentaReceptor", dto.TipoCuentaReceptor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaReceptor", dto.CuentaReceptor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreReceptor", dto.NombreReceptor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RFCReceptor", dto.RFCReceptor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCorte", dto.NumCorte));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MotivoDevolucion", dto.MotivoDevolucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumeroCarga", dto.NumeroCarga));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SecuenciaCarga", dto.SecuenciaCarga));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ErrorEnCarga", dto.ErrorEnCarga));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SolicitudConfirmacion", dto.SolicitudConfirmacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@EsDevolucionExtemporanea", dto.EsDevolucionExtemporanea));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@EstadoBTMUM", dto.EstadoBTMUM));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@EstadoSistemaTEF", dto.EstadoSistemaTEF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ReferenciaConfirmacion", dto.ReferenciaConfirmacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@EsDevolucionRecuperada", dto.EsDevolucionRecuperada));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", dto.NF_ESTATUS));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", dto.T24_DEBIT_AMOUNT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_CURRENCY", dto.T24_DEBIT_CURRENCY));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", dto.T24_BEN_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", dto.T24_CUSTOMER_CODE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_COMPANY_NAME", dto.T24_COMPANY_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO_CUENTA", dto.NF_TIPO_CUENTA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CREDIT_ACCT_NO", dto.T24_CREDIT_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", dto.T24_DEBIT_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_THEIR_REF", dto.T24_DEBIT_THEIR_REF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CREDIT_THEIR_REF", dto.T24_CREDIT_THEIR_REF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_PAYMENT_DETAILS", dto.T24_PAYMENT_DETAILS));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_SATEL_SYS_REF", dto.T24_SATEL_SYS_REF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_INSTRUCTIONS", dto.T24_INSTRUCTIONS));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", dto.NF_FOLIO_UNICO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", dto.NF_BANCO_BENEF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", dto.NF_FECHA_EJECUCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BEN_CUST_NAME", dto.NF_BEN_CUST_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO_OPERACION", dto.NF_TIPO_OPERACION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CODIGO_OPERACION", dto.NF_CODIGO_OPERACION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_APLICACION", dto.NF_FECHA_APLICACION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CARGO", dto.NF_FECHA_CARGO));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            if (!r.Exito)
                return false;

            return ObtenerSalidaLegacy(salidaParam) == 0;
        }

        /// <summary>
        /// Valida si el folio ya fue insertado en TOMI.
        /// </summary>
        public async Task<bool> ExisteFormatoEntradaTef(long idOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ExisteRegistroDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_EXISTE_FORMATO_ENTRADA_TEF,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IDOperacion", idOperacion));
                });

            return (r.Data?.FirstOrDefault()?.NumRegs ?? 0) > 0;
        }

        /// <summary>
        /// Obtiene datos base para construir la entrada/salida.
        /// </summary>
        public async Task<EntTefFormatoTomiDto> ObtenerFormatoEntradaTef(long idOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<EntTefFormatoTomiDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_OBTENER_FORMATO_ENTRADA_TEF,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IDOperacion", idOperacion));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalida(EntTefEntradaSalidaDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", dto.Referencia));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", dto.Num_Cliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", dto.CDL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", dto.Ordenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", dto.Operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", dto.OperacionDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", dto.Tipo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", dto.TipoDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", dto.Cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", dto.Nombre));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", dto.CuentaMoneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", dto.Via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", dto.ViaDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", dto.Saldo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", dto.Comentarios));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", dto.Fecha_Captura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", dto.FechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", dto.Hora));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", dto.Capturista));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", dto.Fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", dto.Referencia_Control));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", dto.Referencia_Control_Cont));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            if (!r.Exito)
                return false;

            return ObtenerSalidaLegacy(salidaParam) == 0;
        }

        private static int ObtenerSalidaLegacy(IDbDataParameter salidaParam)
        {
            if (salidaParam == null || salidaParam.Value == null || salidaParam.Value == DBNull.Value)
                return -1;

            return Convert.ToInt32(salidaParam.Value);
        }

        /// <summary>
        /// Valida si ya existe la entrada/salida por folio único.
        /// </summary>
        public async Task<bool> ExisteEntradaSalidaPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<ExisteRegistroDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_EXISTE_ENTRADA_SALIDA_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return (r.Data?.FirstOrDefault()?.NumRegs ?? 0) > 0;
        }

        /// <summary>
        /// Cambia NF_ESTATUS de PEND a SCNP.
        /// </summary>
        public async Task<bool> ActualizarEstatusFormatoEntradaTefAScnp(long idOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_ACTUALIZAR_ESTATUS_FORMATO_ENTRADA_TEF_A_SCNP,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IDOperacion", idOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cancela el maestro por devolución o cuenta no encontrada.
        /// </summary>
        public async Task<bool> CancelarMaestroPorDevolucion(
            long folio,
            string tipoOperacion,
            string razonRechazo,
            int fechaValor,
            string moneda,
            decimal importe)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_CANCELAR_MAESTRO_POR_DEVOLUCION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RazonRechazo", razonRechazo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca la operación como RECIBIDA - NO ABONADA.
        /// </summary>
        public async Task<bool> MarcarMaestroRecibidaNoAbonada(long folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_MARCAR_MAESTRO_RECIBIDA_NO_ABONADA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene FechaAplicacion e Importe para conciliación TOMI.
        /// </summary>
        public async Task<EntTefConciliacionTomiDto> ObtenerConciliacionFormatoEntradaTef(long idOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<EntTefConciliacionTomiDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_OBTENER_CONCILIACION_FORMATO_ENTRADA_TEF,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IDOperacion", idOperacion));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene fecha valor, moneda e importe para conciliación de entrada/salida.
        /// </summary>
        public async Task<EntTefConciliacionEntradaSalidaDto> ObtenerConciliacionEntradaSalida(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<EntTefConciliacionEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosENTTEF.USP_LECTURA_ENT_TEF_OBTENER_CONCILIACION_ENTRADA_SALIDA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault();
        }
    }
}
using Aplicacion.DTOs.FXD;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXD;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXD
{
    /// <summary>
    /// Representa FXDCms Dao.
    /// </summary>
    public class FXDCmsDao : RepositorioMaestro, IFXDCmsDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDCmsDao.
        /// </summary>
        public FXDCmsDao()
        : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null) { }

        /// <summary>
        /// Obtiene el formato FXD CMS por folio.
        /// </summary>
        public async Task<FormatoFxdCmsDto> ObtenerFormatoFxdCmsPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FormatoFxdCmsDto>(
                ConstantesServicio.ProcedimientosFXD.MUFG_MX2_LECTURA_FXD_OBTENER_FORMATO_FXD_CMS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el formato CMS como rechazado por Forex cancelado.
        /// </summary>
        public async Task<bool> ActualizarFormatoCmsRechazadoPorForexCancelado(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.MUFG_MX2_LECTURA_FXD_ACTUALIZAR_FORMATO_CMS_RECHAZADO_POR_FOREX_CANCELADO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el formato CMS como aceptado hoy.
        /// </summary>
        public async Task<bool> ActualizarFormatoCmsAceptadoHoy(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.MUFG_MX2_LECTURA_FXD_ACTUALIZAR_FORMATO_CMS_ACEPTADO_HOY,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el formato CMS como pendiente de ejecución.
        /// </summary>
        public async Task<bool> ActualizarFormatoCmsPendienteEjecucion(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.MUFG_MX2_LECTURA_FXD_ACTUALIZAR_FORMATO_CMS_PENDIENTE_EJECUCION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

    }
}
using Aplicacion.DTOs.FXD;
using Aplicacion.Enums;
using Aplicacion.Interfaces.CNT;
using Aplicacion.Interfaces.FXD;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXD
{
    /// <summary>
    /// Representa FXDDao.
    /// </summary>
    public class FXDDao : RepositorioMaestro, IFXDDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDDao.
        /// </summary>
        public FXDDao()
        : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene el estatus Forex por referencia externa.
        /// </summary>
        public async Task<ForexStatusDto> ObtenerForexStatusPorExternalRef(string externalRef)
        {
            var r = await EjecutarStoredProcedureQuery<ForexStatusDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_OBTENER_FOREX_STATUS_POR_EXTERNAL_REF,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ExternalRef", externalRef));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza los datos del plan de trabajo maestro en lectura.
        /// </summary>
        public async Task<bool> ActualizarDatosPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string estatus,
            string folioUnico,
            int fechaValor,
            string moneda,
            decimal importe,
            string numCliente,
            string nombreCliente,
            string numCuenta,
            string subtipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_ACTUALIZAR_DATOS_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Estatus", estatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", nombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", numCuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", subtipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cuenta registros TOMBIN FXD por folio.
        /// </summary>
        public async Task<int> ContarRegistrosTombinFxdPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_CONTAR_REGISTROS_TOMBIN_FXD_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Cuenta entradas/salidas por referencia.
        /// </summary>
        public async Task<int> ContarEntradasSalidasPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<ConteoRegistrosDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_CONTAR_ENTRADAS_SALIDAS_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
        }

        /// <summary>
        /// Actualiza TOMBIN FXD como rechazado por Forex cancelado.
        /// </summary>
        public async Task<bool> ActualizarTombinFxdRechazadoPorForexCancelado(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_ACTUALIZAR_TOMBIN_FXD_RECHAZADO_POR_FOREX_CANCELADO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro como cancelado por Forex.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroCanceladoPorForex(
            string folio,
            string tipoOperacion,
            int fechaValor,
            string moneda,
            decimal importe,
            string descripcionError)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_CANCELADO_POR_FOREX,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@DescripcionError", descripcionError));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza TOMBIN FXD como confirmado hoy.
        /// </summary>
        public async Task<bool> ActualizarTombinFxdConfirmadoHoy(string folio, string usuarioConfirma, DateTime fechaConfirma)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_ACTUALIZAR_TOMBIN_FXD_CONFIRMADO_HOY,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@UsuarioConfirma", usuarioConfirma));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaConfirma", fechaConfirma));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza TOMBIN FXD como pendiente por fecha futura.
        /// </summary>
        public async Task<bool> ActualizarTombinFxdPendienteFechaFutura(string folio, string usuarioConfirma, DateTime fechaConfirma)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_ACTUALIZAR_TOMBIN_FXD_PENDIENTE_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@UsuarioConfirma", usuarioConfirma));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaConfirma", fechaConfirma));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene TOMBIN FXD para conciliación.
        /// </summary>
        public async Task<TombinFxdConciliacionDto> ObtenerTombinFxdParaConciliacion(string folio, string estatusCsv)
        {
            var r = await EjecutarStoredProcedureQuery<TombinFxdConciliacionDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_OBTENER_TOMBIN_FXD_PARA_CONCILIACION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@EstatusCsv", estatusCsv));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene entrada/salida por referencia.
        /// </summary>
        public async Task<FxdEntradaSalidaDto> ObtenerEntradaSalidaPorReferencia(string referencia)
        {
            var r = await EjecutarStoredProcedureQuery<FxdEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_OBTENER_ENTRADA_SALIDA_POR_REFERENCIA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene TOMBIN FXD por folio.
        /// </summary>
        public async Task<TombinFxdEntradaSalidaDto> ObtenerTombinFxdPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<TombinFxdEntradaSalidaDto>(
                ConstantesServicio.ProcedimientosFXD.USP_LECTURA_FXD_OBTENER_TOMBIN_FXD_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoFxdEnTomi(
            string nfFolioUnico,
            string nfTipo,
            long nfFolio,
            string t24CustomerCode,
            string t24DebitAcctNo,
            string nfEstatus,
            string nfUsuarioCaptura,
            string nfUsuarioAutoriza,
            DateTime nfFechaCaptura,
            DateTime? nfFechaAutoriza,
            decimal t24DebitAmount,
            string t24ShortName,
            string nfForexRelacionado,
            string t24DebitCurrency,
            string nfBancoBenef,
            string t24BenAcctNo,
            string nfTipoCuenta,
            string t24BenCustName,
            string nfRfcBeneficiario,
            string nfTasaRetencion,
            int nfFechaEjecucion,
            string t24DebitTheirRef,
            decimal nfMontoEnviar,
            string idPlantilla,
            long? idFormatoPlantilla,
            decimal? opRefNumerica)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_TOMBIN_INSERT_FORMATO_FXD,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", nfFolioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO", nfTipo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO", nfFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", t24CustomerCode));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", t24DebitAcctNo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", nfEstatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_CAPTURA", nfUsuarioCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_AUTORIZA", nfUsuarioAutoriza));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CAPTURA", nfFechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_AUTORIZA", nfFechaAutoriza.HasValue ? (object)nfFechaAutoriza.Value : DBNull.Value));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", t24DebitAmount));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_SHORT_NAME", t24ShortName));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOREX_RELACIONADO", nfForexRelacionado));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_CURRENCY", t24DebitCurrency));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", nfBancoBenef));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", t24BenAcctNo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO_CUENTA", nfTipoCuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_CUST_NAME", t24BenCustName));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_RFC_BENEFICIARIO", nfRfcBeneficiario));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TASA_RETENCION", nfTasaRetencion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", nfFechaEjecucion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_THEIR_REF", t24DebitTheirRef));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MONTO_ENVIAR", nfMontoEnviar));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_PLANTILLA", idPlantilla));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_FORMATO_PLANTILLA", idFormatoPlantilla.HasValue ? (object)idFormatoPlantilla.Value : DBNull.Value));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OP_REF_NUMERICA", opRefNumerica.HasValue ? (object)opRefNumerica.Value : DBNull.Value));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            if (!r.Exito)
                return false;

            return ObtenerSalidaLegacy(salidaParam) == 0;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalidaSrv(
            string referencia,
            int numCliente,
            double cdl,
            string ordenante,
            double operacion,
            string operacionDesc,
            string tipo,
            string tipoDesc,
            double cuenta,
            string nombre,
            string cuentaMoneda,
            double via,
            string viaDesc,
            double importe,
            double saldo,
            string comentarios,
            DateTime fechaCaptura,
            DateTime fechaValor,
            string hora,
            string capturista,
            double fad,
            string referenciaControl,
            double referenciaControlCont)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXD.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", referencia));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", numCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", cdl));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", ordenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", operacionDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", tipo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", tipoDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", nombre));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", cuentaMoneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", viaDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", saldo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", comentarios));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", fechaCaptura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", hora));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", capturista));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", referenciaControl));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", referenciaControlCont));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            if (!r.Exito)
                return false;

            return ObtenerSalidaLegacy(salidaParam) == 0;
        }

        private static int ObtenerSalidaLegacy(IDbDataParameter salidaParam)
        {
            if (salidaParam == null || salidaParam.Value == null || salidaParam.Value == DBNull.Value)
                return -1;

            return Convert.ToInt32(salidaParam.Value);
        }

    }
}
using Aplicacion.DTOs.FXD;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXD;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXD
{
    /// <summary>
    /// Representa FXDGPHDao.
    /// </summary>
    public class FXDGPHDao : RepositorioMaestro, IFXDGPHDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDGPHDao.
        /// </summary>
        public FXDGPHDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos de FXD GPH asociados al folio solicitado.
        /// </summary>
        public async Task<FxdGphDatosDto> ObtenerDatosFxdGphPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxdGphDatosDto>(
                ConstantesServicio.ProcedimientosFXDGPH.USP_LECTURA_FXD_GPH_OBTENER_DATOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el folio único de FXD GPH asociado al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarFolioUnicoFxdGph(string folio, string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXDGPH.USP_LECTURA_FXD_GPH_ACTUALIZAR_FOLIO_UNICO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro con datos de FXD GPH asociados al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroDesdeFxdGph(
            string folio,
            string tipoOperacion,
            string folioUnico,
            int fechaValor,
            string monedaDefault)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXDGPH.USP_LECTURA_FXD_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefault", monedaDefault));
                },
                validarAfectacion: false);

            return r.Exito;
        }

    }
}
using Aplicacion.DTOs.FXD;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXD;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXD
{
    /// <summary>
    /// Representa FXDH2 HDao.
    /// </summary>
    public class FXDH2HDao : RepositorioMaestro, IFXDH2HDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDH2HDao.
        /// </summary>
        public FXDH2HDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos de FXD H2H asociados al folio solicitado.
        /// </summary>
        public async Task<FxdH2hDatosDto> ObtenerDatosFxdH2hPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxdH2hDatosDto>(
                ConstantesServicio.ProcedimientosFXDH2H.USP_LECTURA_FXD_H2H_OBTENER_DATOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro con datos de FXD H2H asociados al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroDesdeFxdH2h(
            string folio,
            string tipoOperacion,
            string folioUnico,
            int fechaValor,
            string monedaDefault)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXDH2H.USP_LECTURA_FXD_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefault", monedaDefault));
                },
                validarAfectacion: false);

            return r.Exito;
        }

    }
}
using Aplicacion.DTOs.FXD_Local;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXDLocal;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXDLocal
{
    /// <summary>
    /// Representa FXDLocal Dao.
    /// </summary>
    public class FXDLocalDao : RepositorioMaestro, IFXDLocalDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDLocalDao.
        /// </summary>
        public FXDLocalDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Valida si el folio corresponde a un mensaje PACS008.
        /// </summary>
        public async Task<bool> EsPacs008PorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxdLocalEsPacs008Dto>(
                ConstantesServicio.ProcedimientosFXDLocal.USP_LECTURA_FXD_LOCAL_ES_PACS008_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return (r.Data?.FirstOrDefault()?.EsPacs008 ?? 0) == 1;
        }

        /// <summary>
        /// Obtiene los datos básicos de FXD Local por folio.
        /// </summary>
        public async Task<FxdLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxdLocalDatosBasicosDto>(
                ConstantesServicio.ProcedimientosFXDLocal.USP_LECTURA_FXD_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el folio único de FXD Local asociado al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarFolioUnico(string folio, string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXDLocal.USP_LECTURA_FXD_LOCAL_ACTUALIZAR_FOLIO_UNICO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro en lectura para FXD Local.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string monedaDefaultMn,
            string fechaEjecucion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXDLocal.USP_LECTURA_FXD_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaEjecucion", fechaEjecucion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene los datos de comisión de FXD Local por folio.
        /// </summary>
        public async Task<FxdLocalDatosComisionDto> ObtenerDatosComisionPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxdLocalDatosComisionDto>(
                ConstantesServicio.ProcedimientosFXDLocal.USP_LECTURA_FXD_LOCAL_OBTENER_DATOS_COMISION_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza los datos de comisión de FXD Local.
        /// </summary>
        public async Task<bool> ActualizarDatosComision(
            long folio,
            decimal montoSpeiEnvio,
            string commissionType,
            string commissionCode,
            string chargeCode)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXDLocal.USP_LECTURA_FXD_LOCAL_ACTUALIZAR_DATOS_COMISION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MontoSpeiEnvio", montoSpeiEnvio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CommissionType", commissionType));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CommissionCode", commissionCode));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ChargeCode", chargeCode));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs.FXD_Local;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXDLocal;
using MUFG_T24.Comunes;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXDLocal
{
    /// <summary>
    /// Representa FXDLocal T24 Dao.
    /// </summary>
    public class FXDLocalT24Dao : RepositorioMaestro, IFXDLocalT24Dao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXDLocalT24Dao.
        /// </summary>
        public FXDLocalT24Dao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW, null, null) { }

        /// <summary>
        /// Valida si el customer code corresponde a banco en T24.
        /// </summary>
        public async Task<bool> EsBancoPorCustomerCode(string customerCode)
        {
            var r = await EjecutarStoredProcedureQuery<FxdLocalEsBancoDto>(
                ConstantesServicio.ProcedimientosFXDLocal.MUFG_MX2_LECTURA_FXD_LOCAL_ES_BANCO_POR_CUSTOMER_CODE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CustomerCode", customerCode));
                });

            return (r.Data?.FirstOrDefault()?.EsBanco ?? 0) == 1;
        }

        /// <summary>
        /// Obtiene el tipo de comisión T24 por identificador.
        /// </summary>
        public async Task<FxdLocalCommissionTypeT24Dto> ObtenerCommissionTypeT24(string commissionType)
        {
            var r = await EjecutarStoredProcedureQuery<FxdLocalCommissionTypeT24Dto>(
                ConstantesServicio.ProcedimientosFXDLocal.MUFG_MX2_LECTURA_FXD_LOCAL_OBTENER_COMMISSION_TYPE_T24,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CommissionType", commissionType));
                });

            return r.Data?.FirstOrDefault();
        }
    }
}
using Aplicacion.DTOs.FXI;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXI;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Infraestructura.Repositorio;

namespace Infraestructura.Repositorio.FXI
{
    /// <summary>
    /// Representa Fxi Tomi Dao.
    /// </summary>
    public class FxiTomiDao : RepositorioMaestro, IFxiTomiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FxiTomiDao.
        /// </summary>
        public FxiTomiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene el estatus Forex por referencia externa.
        /// </summary>
        public async Task<FxiForexEstatusTomiDto> ObtenerEstatusForex(string externalRef)
        {
            var r = await EjecutarStoredProcedureQuery<FxiForexEstatusTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_OBTENER_ESTATUS_FOREX,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ExternalRef", externalRef));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene formatos FXI por folio.
        /// </summary>
        public async Task<List<FxiTombinNfFxiTomiDto>> ObtenerFormatosFxi(long folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxiTombinNfFxiTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_OBTENER_FORMATO_FXI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.ToList() ?? new List<FxiTombinNfFxiTomiDto>();
        }

        /// <summary>
        /// Obtiene la cuenta Nostro por identificador y moneda.
        /// </summary>
        public async Task<FxiNostroCuentaTomiDto> ObtenerNostro(string nostroAccountIdentifier, string nostroAccountCurrency)
        {
            var r = await EjecutarStoredProcedureQuery<FxiNostroCuentaTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_OBTENER_NOSTRO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NostroAccountIdentifier", nostroAccountIdentifier));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NostroAccountCurrency", nostroAccountCurrency));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza los datos generales del maestro FXI.
        /// </summary>
        public async Task<bool> ActualizarMaestroDatosGenerales(FxiActualizarMaestroDatosGeneralesTomiDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_ACTUALIZAR_MAESTRO_DATOS_GENERALES,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", dto.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", dto.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", dto.FolioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", dto.FechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", dto.Moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", dto.NumCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", dto.NombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", dto.NumCuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", dto.SubtipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoFxi(FxiInsertarFormatoFxiTomiDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_TOMBIN_INSERT_FORMATO_FXI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", dto.NF_FOLIO_UNICO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO", dto.NF_TIPO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO", dto.NF_FOLIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", dto.T24_CUSTOMER_CODE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", dto.T24_DEBIT_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", dto.NF_ESTATUS));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_CAPTURA", dto.NF_USUARIO_CAPTURA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_AUTORIZA", dto.NF_USUARIO_AUTORIZA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CAPTURA", dto.NF_FECHA_CAPTURA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_AUTORIZA", dto.NF_FECHA_AUTORIZA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", dto.T24_DEBIT_AMOUNT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_SHORT_NAME", dto.T24_SHORT_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_CURRENCY", dto.T24_DEBIT_CURRENCY));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOREX_RELACIONADO", dto.NF_FOREX_RELACIONADO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MONTO_ENVIAR", dto.NF_MONTO_ENVIAR));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_THEIR_REF", dto.T24_DEBIT_THEIR_REF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO_CUENTA", dto.NF_TIPO_CUENTA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_PAYMENT", dto.NF_PAYMENT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CHARGE", dto.NF_CHARGE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", dto.NF_FECHA_EJECUCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_CUST_NAME", dto.T24_BEN_CUST_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FXD_COMMENT", dto.NF_FXD_COMMENT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_RFC_BENEFICIARIO", dto.NF_RFC_BENEFICIARIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TASA_RETENCION", dto.NF_TASA_RETENCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", dto.NF_BANCO_BENEF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_SWIFT_BENEFICIARIO", dto.NF_SWIFT_BENEFICIARIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", dto.T24_BEN_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_CORRESPONSAL", dto.NF_BANCO_CORRESPONSAL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_SWIFT_CORRESPONSAL", dto.NF_SWIFT_CORRESPONSAL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CUENTA_CORRESPONSAL", dto.NF_CUENTA_CORRESPONSAL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MENSAJE", dto.NF_MENSAJE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_COUNTRY", dto.NF_COUNTRY));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_PLANTILLA", dto.ID_PLANTILLA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_FORMATO_PLANTILLA", dto.ID_FORMATO_PLANTILLA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@COUNTRY_BEN", dto.COUNTRY_BEN));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@city_ben", dto.city_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@street_ben", dto.street_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@colony_ben", dto.colony_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@building_number_ben", dto.building_number_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@post_code_ben", dto.post_code_ben));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && OutputParameterHelper.EsSalidaCero(salidaParam, ConstantesServicio.ProcedimientosFXI.USP_TOMBIN_INSERT_FORMATO_FXI);
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoFxe(FxiInsertarFormatoFxiTomiDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_TOMBIN_INSERT_FORMATO_FXE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", dto.NF_FOLIO_UNICO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO", dto.NF_TIPO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO", dto.NF_FOLIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", dto.T24_CUSTOMER_CODE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", dto.T24_DEBIT_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", dto.NF_ESTATUS));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_CAPTURA", dto.NF_USUARIO_CAPTURA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_AUTORIZA", dto.NF_USUARIO_AUTORIZA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CAPTURA", dto.NF_FECHA_CAPTURA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_AUTORIZA", dto.NF_FECHA_AUTORIZA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", dto.T24_DEBIT_AMOUNT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_SHORT_NAME", dto.T24_SHORT_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_CURRENCY", dto.T24_DEBIT_CURRENCY));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOREX_RELACIONADO", dto.NF_FOREX_RELACIONADO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MONTO_ENVIAR", dto.NF_MONTO_ENVIAR));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_THEIR_REF", dto.T24_DEBIT_THEIR_REF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO_CUENTA", dto.NF_TIPO_CUENTA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_PAYMENT", dto.NF_PAYMENT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CHARGE", dto.NF_CHARGE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", dto.NF_FECHA_EJECUCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_CUST_NAME", dto.T24_BEN_CUST_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FXD_COMMENT", dto.NF_FXD_COMMENT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_RFC_BENEFICIARIO", dto.NF_RFC_BENEFICIARIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_RFC_ORDENANTE", dto.NF_RFC_ORDENANTE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TASA_RETENCION", dto.NF_TASA_RETENCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", dto.NF_BANCO_BENEF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_SWIFT_BENEFICIARIO", dto.NF_SWIFT_BENEFICIARIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", dto.T24_BEN_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_CORRESPONSAL", dto.NF_BANCO_CORRESPONSAL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_SWIFT_CORRESPONSAL", dto.NF_SWIFT_CORRESPONSAL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CUENTA_CORRESPONSAL", dto.NF_CUENTA_CORRESPONSAL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MENSAJE", dto.NF_MENSAJE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_COUNTRY", dto.NF_COUNTRY));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_PLANTILLA", dto.ID_PLANTILLA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_FORMATO_PLANTILLA", dto.ID_FORMATO_PLANTILLA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@COUNTRY_BEN", dto.COUNTRY_BEN));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@city_ben", dto.city_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@street_ben", dto.street_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@colony_ben", dto.colony_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@building_number_ben", dto.building_number_ben));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@post_code_ben", dto.post_code_ben));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int16);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && OutputParameterHelper.EsSalidaCero(salidaParam, ConstantesServicio.ProcedimientosFXI.USP_TOMBIN_INSERT_FORMATO_FXE);
        }

        /// <summary>
        /// Valida si ya existe el formato FXI.
        /// </summary>
        public async Task<bool> ExisteFormatoFxi(long folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxiExisteRegistroTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_EXISTE_FORMATO_FXI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return (r.Data?.FirstOrDefault()?.num_regs ?? 0) > 0;
        }

        /// <summary>
        /// Obtiene el formato FXI por folio.
        /// </summary>
        public async Task<FxiTombinNfFxiTomiDto> ObtenerFormatoFxi(long folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxiTombinNfFxiTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_OBTENER_FORMATO_FXI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarEntradaSalida(FxiInsertarEntradaSalidaTomiDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_TOMBIN_INSERT_SRV_ENTRADAS_SALIDAS,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia", dto.Referencia));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Num_Cliente", dto.Num_Cliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CDL", dto.CDL));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Ordenante", dto.Ordenante));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Operacion", dto.Operacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@OperacionDesc", dto.OperacionDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", dto.Tipo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoDesc", dto.TipoDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Cuenta", dto.Cuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Nombre", dto.Nombre));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaMoneda", dto.CuentaMoneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Via", dto.Via));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ViaDesc", dto.ViaDesc));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Saldo", dto.Saldo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Comentarios", dto.Comentarios));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fecha_Captura", dto.Fecha_Captura));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", dto.FechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Hora", dto.Hora));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Capturista", dto.Capturista));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Fad", dto.Fad));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control", dto.Referencia_Control));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Referencia_Control_Cont", dto.Referencia_Control_Cont));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int16);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && int.Parse(salidaParam.Value.ToString()) == 0;
        }

        /// <summary>
        /// Valida si ya existe la entrada/salida.
        /// </summary>
        public async Task<bool> ExisteEntradaSalida(string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<FxiExisteRegistroTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_EXISTE_ENTRADA_SALIDA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                });

            return (r.Data?.FirstOrDefault()?.num_regs ?? 0) > 0;
        }

        /// <summary>
        /// Actualiza el formato FXI como rechazado por Forex.
        /// </summary>
        public async Task<bool> ActualizarFormatoFxiRechazadoForex(long folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_ACTUALIZAR_FORMATO_FXI_RECHAZADO_FOREX,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cancela el maestro por Forex.
        /// </summary>
        public async Task<bool> CancelarMaestroForex(FxiCancelarMaestroForexTomiDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_CANCELAR_MAESTRO_FOREX,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", dto.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", dto.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", dto.FechaValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Moneda", dto.Moneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ErrorDescripcion", dto.ErrorDescripcion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el estatus del formato FXI.
        /// </summary>
        public async Task<bool> ActualizarEstatusFormatoFxi(FxiActualizarEstatusNfFxiTomiDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_ACTUALIZAR_ESTATUS_FORMATO_FXI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", dto.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Estatus", dto.Estatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@UsuarioConfirma", dto.UsuarioConfirma));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaConfirma", dto.FechaConfirma));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene los datos de conciliación del formato FXI.
        /// </summary>
        public async Task<FxiConciliacionNfFxiTomiDto> ObtenerDatosConciliacionFormatoFxi(long folio, bool forexCancelado)
        {
            var r = await EjecutarStoredProcedureQuery<FxiConciliacionNfFxiTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_OBTENER_DATOS_CONCILIACION_FORMATO_FXI,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ForexCancelado", forexCancelado));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Obtiene los datos de conciliación de entrada/salida.
        /// </summary>
        public async Task<FxiEntradaSalidaConciliacionTomiDto> ObtenerDatosConciliacionEntradaSalida(string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<FxiEntradaSalidaConciliacionTomiDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_OBTENER_DATOS_CONCILIACION_ENTRADA_SALIDA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el maestro como FECHA FUTURA.
        /// </summary>
        public async Task<bool> ActualizarMaestroFechaFutura(long folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_LECTURA_FXI_ACTUALIZAR_MAESTRO_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarRegistroSpeiCobertura(long folio)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXI.USP_INSERT_SPEI_REGISTRATION_FOR_COVER,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@folio", folio));
                });

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs;
using Aplicacion.DTOs.FXIGPH;
using Aplicacion.Enums;
using Infraestructura.Repositorio.FXIGPH;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXIGPH
{
    /// <summary>
    /// Representa FXI GPHDao.
    /// </summary>
    public class FXI_GPHDao : RepositorioMaestro, IFXI_GPHDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXI_GPHDao.
        /// </summary>
        public FXI_GPHDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos básicos de FXI GPH por folio.
        /// </summary>
        public async Task<FxiGphDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxiGphDatosBasicosDto>(
                ConstantesServicio.ProcedimientosFXIGPH.USP_LECTURA_FXI_GPH_OBTENER_DATOS_BASICOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el folio único de FXI GPH asociado al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarFolioUnico(string folio, string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXIGPH.USP_LECTURA_FXI_GPH_ACTUALIZAR_FOLIO_UNICO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro en lectura para FXI GPH.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string monedaDefaultMn)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXIGPH.USP_LECTURA_FXI_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca el plan de trabajo maestro como FECHA FUTURA.
        /// </summary>
        public async Task<bool> MarcarFechaFutura(string folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXIGPH.USP_LECTURA_FXI_GPH_MARCAR_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }


        /// <summary>
        /// Inserta el detalle de operación GPH, reemplazando el detalle existente para la misma llave.
        /// </summary>
        public async Task<bool> InsertarDetalleOperacionGph(
            string archivo,
            long folioArchivo,
            string idOperacion,
            string tipoOperacion,
            string estatus,
            string error)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXIGPH.USP_LECTURA_FXI_GPH_INSERTAR_DETALLE_OPERACION_GPH,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Archivo", archivo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioArchivo", folioArchivo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdOperacion", idOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Estatus", estatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Error", error));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs.FXI_H2H;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXIGPH;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Aplicacion.Interfaces.FXI_H2H
{
    /// <summary>
    /// Representa FXI H2 HDao.
    /// </summary>
    public class FXI_H2HDao : RepositorioMaestro, IFXI_H2HDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXI_H2HDao.
        /// </summary>
        public FXI_H2HDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos básicos de FXI H2H por folio.
        /// </summary>
        public async Task<FxiH2HDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxiH2HDatosBasicosDto>(
                ConstantesServicio.ProcedimientosFXIH2H.USP_LECTURA_FXI_H2H_OBTENER_DATOS_BASICOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro en lectura para FXI H2H.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string monedaDefaultMn)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXIH2H.USP_LECTURA_FXI_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca el plan de trabajo maestro como FECHA FUTURA.
        /// </summary>
        public async Task<bool> MarcarFechaFutura(string folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXIH2H.USP_LECTURA_FXI_H2H_MARCAR_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

    }
}
using Aplicacion.DTOs.FXT;
using Aplicacion.Enums;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXT
{
    /// <summary>
    /// Representa Fxt Tomi Dao.
    /// </summary>
    public class FxtTomiDao : RepositorioMaestro, IFxtTomiDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FxtTomiDao.
        /// </summary>
        public FxtTomiDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene el estatus Forex por referencia externa.
        /// </summary>
        public async Task<FxtForexStatusTomiDto> ObtenerStatusForexPorExternalRef(string externalRef)
        {
            var r = await EjecutarStoredProcedureQuery<FxtForexStatusTomiDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_OBTENER_STATUS_FOREX_POR_EXTERNAL_REF,
                cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@ExternalRef", externalRef)); });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza los datos generales del plan de trabajo.
        /// </summary>
        public async Task<bool> ActualizarDatosGeneralesPlanTrabajo(FxtActualizarDatosGeneralesPlanTrabajoDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_ACTUALIZAR_DATOS_GENERALES_PLAN_TRABAJO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", dto.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", dto.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", dto.FolioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FecValor", dto.FecValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CveMoneda", dto.CveMoneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCliente", dto.NumCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", dto.NombreCliente));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumCuenta", dto.NumCuenta));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@SubtipoOperacion", dto.SubtipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Procedimiento legacy.
        /// </summary>
        public async Task<bool> InsertarFormatoFxt(FxtInsertarFormatoFxtTomiDto dto)
        {
            IDbDataParameter salidaParam = null;

            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.USP_TOMBIN_INSERT_FORMATO_FXT,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", dto.NF_FOLIO_UNICO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO", dto.NF_TIPO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO", dto.NF_FOLIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", dto.T24_CUSTOMER_CODE));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", dto.T24_DEBIT_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", dto.NF_ESTATUS));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_CAPTURA", dto.NF_USUARIO_CAPTURA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_AUTORIZA", dto.NF_USUARIO_AUTORIZA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CAPTURA", dto.NF_FECHA_CAPTURA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_AUTORIZA", dto.NF_FECHA_AUTORIZA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", dto.T24_DEBIT_AMOUNT));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_SHORT_NAME", dto.T24_SHORT_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOREX_RELACIONADO", dto.NF_FOREX_RELACIONADO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_CURRENCY", dto.T24_DEBIT_CURRENCY));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_BANCO_BENEF", dto.NF_BANCO_BENEF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_ACCT_NO", dto.T24_BEN_ACCT_NO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO_CUENTA", dto.NF_TIPO_CUENTA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_BEN_CUST_NAME", dto.T24_BEN_CUST_NAME));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_RFC_BENEFICIARIO", dto.NF_RFC_BENEFICIARIO));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TASA_RETENCION", dto.NF_TASA_RETENCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", dto.NF_FECHA_EJECUCION));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_THEIR_REF", dto.T24_DEBIT_THEIR_REF));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MONTO_ENVIAR", dto.NF_MONTO_ENVIAR));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_PLANTILLA", dto.ID_PLANTILLA));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ID_FORMATO_PLANTILLA", dto.ID_FORMATO_PLANTILLA));

                    salidaParam = basedatosConexion.CrearParametroSalida("@Salida", DbType.Int32);
                    salidaParam.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(salidaParam);
                });

            return r.Exito && int.Parse(salidaParam.Value.ToString()) == 0;
        }

        /// <summary>
        /// Valida si existe el formato FXT por folio.
        /// </summary>
        public async Task<bool> ExisteFormatoFxtPorFolio(long folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxtExisteRegistroDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_EXISTE_FORMATO_FXT_POR_FOLIO,
                cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio)); });

            return (r.Data?.FirstOrDefault()?.NumRegs ?? 0) > 0;
        }

        /// <summary>
        /// Actualiza el estatus FXT como rechazado por Forex.
        /// </summary>
        public async Task<bool> ActualizarEstatusFxtRechazadoForex(FxtActualizarEstatusFxtRechazadoForexDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_ACTUALIZAR_ESTATUS_FXT_RECHAZADO_FOREX,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NfFolio", dto.NfFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NfRazon", dto.NfRazon));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el estatus FXT de confirmación.
        /// </summary>
        public async Task<bool> ActualizarEstatusFxtConfirmacion(FxtActualizarEstatusFxtConfirmacionDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_ACTUALIZAR_ESTATUS_FXT_CONFIRMACION,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NfFolio", dto.NfFolio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NfEstatus", dto.NfEstatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NfUsuarioConfirma", dto.NfUsuarioConfirma));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@NfFechaConfirma", dto.NfFechaConfirma));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Cancela el plan de trabajo por Forex.
        /// </summary>
        public async Task<bool> CancelarPlanTrabajoPorForex(FxtCancelarPlanTrabajoPorForexDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_CANCELAR_PLAN_TRABAJO_POR_FOREX,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", dto.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", dto.TipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FecValor", dto.FecValor));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@CveMoneda", dto.CveMoneda));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Importe", dto.Importe));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ErrDescripcion", dto.ErrDescripcion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Obtiene datos de conciliación FXT por folio.
        /// </summary>
        public async Task<FxtConciliacionTomiDto> ObtenerConciliacionFxtPorFolio(long folio, bool forexCancelado)
        {
            var r = await EjecutarStoredProcedureQuery<FxtConciliacionTomiDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_OBTENER_CONCILIACION_FXT_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@ForexCancelado", forexCancelado));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Marca el plan de trabajo como FECHA FUTURA.
        /// </summary>
        public async Task<bool> MarcarPlanTrabajoFechaFutura(FxtMarcarFechaFuturaPlanTrabajoDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.USP_LECTURA_FXT_MARCAR_PLAN_TRABAJO_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", dto.Folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", dto.TipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs.FXT;
using Aplicacion.Enums;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXT
{
    /// <summary>
    /// Representa Fxt Cms Dao.
    /// </summary>
    public class FxtCmsDao : RepositorioMaestro, IFxtCmsDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FxtCmsDao.
        /// </summary>
        public FxtCmsDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null) { }

        /// <summary>
        /// Obtiene el formato CMS por folio.
        /// </summary>
        public async Task<FxtFormatoCmsDto> ObtenerFormatoPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxtFormatoCmsDto>(
                ConstantesServicio.ProcedimientosFXT.MUFG_MX2_LECTURA_FXT_OBTENER_FORMATO_POR_FOLIO,
                cmd => { cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio)); });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el estatus del formato CMS como rechazado.
        /// </summary>
        public async Task<bool> ActualizarEstatusFormatoRechazado(FxtActualizarEstatusFormatoRechazadoDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.MUFG_MX2_LECTURA_FXT_ACTUALIZAR_ESTATUS_FORMATO_RECHAZADO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdFormato", dto.IdFormato));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@RazonRechazo", dto.RazonRechazo));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el estatus simple del formato CMS.
        /// </summary>
        public async Task<bool> ActualizarEstatusFormatoSimple(FxtActualizarEstatusFormatoSimpleDto dto)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXT.MUFG_MX2_LECTURA_FXT_ACTUALIZAR_ESTATUS_FORMATO_SIMPLE,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdFormato", dto.IdFormato));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Mensaje", dto.Mensaje));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Estatus", dto.Estatus));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }

}
using Aplicacion.DTOs;
using Aplicacion.DTOs.FXT_GPH;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXT_GPH;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXT_GPH
{
    /// <summary>
    /// Representa FXT GPHDao.
    /// </summary>
    public class FXT_GPHDao : RepositorioMaestro, IFXT_GPHDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXT_GPHDao.
        /// </summary>
        public FXT_GPHDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos bÃ¡sicos de FXT GPH por folio.
        /// </summary>
        public async Task<FxtGphDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxtGphDatosBasicosDto>(
                ConstantesServicio.ProcedimientosFXTGPH.USP_LECTURA_FXT_GPH_OBTENER_DATOS_BASICOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.LastOrDefault();
        }

        /// <summary>
        /// Actualiza el folio Ãºnico de FXT GPH asociado al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarFolioUnico(string folio, string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTGPH.USP_LECTURA_FXT_GPH_ACTUALIZAR_FOLIO_UNICO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro en lectura para FXT GPH.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string monedaDefaultMn)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTGPH.USP_LECTURA_FXT_GPH_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca el plan de trabajo maestro como FECHA FUTURA.
        /// </summary>
        public async Task<bool> MarcarFechaFutura(string folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTGPH.USP_LECTURA_FXT_GPH_MARCAR_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Inserta el detalle de operación GPH, reemplazando el detalle existente para la misma llave.
        /// </summary>
        public async Task<bool> InsertarDetalleOperacionGph(
            string archivo,
            long folioArchivo,
            string idOperacion,
            string tipoOperacion,
            string estatus,
            string error)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTGPH.USP_LECTURA_FXT_GPH_INSERTAR_DETALLE_OPERACION_GPH,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Archivo", archivo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioArchivo", folioArchivo));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdOperacion", idOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Estatus", estatus));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Error", error));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs.FXT_H2H;
using Aplicacion.Enums;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXTH2H
{
    /// <summary>
    /// Representa FXTH2 HDao.
    /// </summary>
    public class FXTH2HDao : RepositorioMaestro, IFXTH2HDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXTH2HDao.
        /// </summary>
        public FXTH2HDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos básicos de FXT H2H por folio.
        /// </summary>
        public async Task<FxtH2HDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxtH2HDatosBasicosDto>(
                ConstantesServicio.ProcedimientosFXTH2H.USP_LECTURA_FXT_H2H_OBTENER_DATOS_BASICOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.FirstOrDefault();
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro en lectura para FXT H2H.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string monedaDefaultMn)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTH2H.USP_LECTURA_FXT_H2H_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Marca el plan de trabajo maestro como FECHA FUTURA.
        /// </summary>
        public async Task<bool> MarcarFechaFutura(string folio, string tipoOperacion)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTH2H.USP_LECTURA_FXT_H2H_MARCAR_FECHA_FUTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs;
using Aplicacion.DTOs.FXT_Local;
using Aplicacion.Enums;
using Aplicacion.Interfaces.FXT_Local;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Linq;
using System.Threading.Tasks;

namespace Infraestructura.Repositorio.FXT_Local
{
    /// <summary>
    /// Representa FXT Local Dao.
    /// </summary>
    public class FXT_LocalDao : RepositorioMaestro, IFXT_LocalDao
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase FXT_LocalDao.
        /// </summary>
        public FXT_LocalDao()
            : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

        /// <summary>
        /// Obtiene los datos bÃ¡sicos de FXT Local por folio.
        /// </summary>
        public async Task<FxtLocalDatosBasicosDto> ObtenerDatosBasicosPorFolio(string folio)
        {
            var r = await EjecutarStoredProcedureQuery<FxtLocalDatosBasicosDto>(
                ConstantesServicio.ProcedimientosFXTLocal.USP_LECTURA_FXT_LOCAL_OBTENER_DATOS_BASICOS_POR_FOLIO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                });

            return r.Data?.LastOrDefault();
        }

        /// <summary>
        /// Actualiza el folio Ãºnico de FXT Local asociado al folio solicitado.
        /// </summary>
        public async Task<bool> ActualizarFolioUnico(string folio, string folioUnico)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTLocal.USP_LECTURA_FXT_LOCAL_ACTUALIZAR_FOLIO_UNICO,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                },
                validarAfectacion: false);

            return r.Exito;
        }

        /// <summary>
        /// Actualiza el plan de trabajo maestro en lectura para FXT Local.
        /// </summary>
        public async Task<bool> ActualizarPlanTrabajoMaestroLectura(
            string folio,
            string tipoOperacion,
            string folioUnico,
            string monedaDefaultMn)
        {
            var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
                ConstantesServicio.ProcedimientosFXTLocal.USP_LECTURA_FXT_LOCAL_ACTUALIZAR_PLAN_TRABAJO_MAESTRO_LECTURA,
                cmd =>
                {
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioUnico", folioUnico));
                    cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaDefaultMn", monedaDefaultMn));
                },
                validarAfectacion: false);

            return r.Exito;
        }
    }
}
using Aplicacion.DTOs.MFX;
using Aplicacion.Enums;
using Aplicacion.Interfaces.MFX;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
/// <summary>
/// Representa MFXCms Dao.
/// </summary>

public class MFXCmsDao : RepositorioMaestro, IMFXCmsDao
{
    /// <summary>
    /// Inicializa una nueva instancia de la clase MFXCmsDao.
    /// </summary>
    public MFXCmsDao()
        : base(BasedatosProveedor.SqlServer, NombreBasedatos.CMS, null, null) { }


    /// <summary>
    /// Obtiene formatos Forex confirmados desde CMS.
    /// </summary>
    public async Task<List<MFXFormatoCmsDto>> ObtenerFormatosForexConfirmados(string folio)
    {
        var r = await EjecutarStoredProcedureQuery<MFXFormatoCmsDto>(
            ConstantesServicio.ProcedimientosMFX.MUFG_MX2_LECTURA_MFX_OBTENER_FORMATOS_FOREX_CONFIRMADOS,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
            });

        return r.Data?.ToList() ?? new List<MFXFormatoCmsDto>();
    }

    /// <summary>
    /// Rechaza el formato Forex en CMS.
    /// </summary>
    public async Task<bool> RechazarFormatoForex(object folioCms)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.MUFG_MX2_LECTURA_MFX_RECHAZAR_FORMATO_FOREX,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
            },
            validarAfectacion: false);

        return r.Exito;
    }


    /// <summary>
    /// Acepta el formato Forex en CMS.
    /// </summary>
    public async Task<bool> AceptarFormatoForex(object folioCms)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.MUFG_MX2_LECTURA_MFX_ACEPTAR_FORMATO_FOREX,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Inserta autorización Forex en CMS desde TOMI.
    /// </summary>
    public async Task<bool> InsertarAutorizacionForexDesdeTomi(
        long idAutorizacion,
        long idCliente,
        string divisaEntrega,
        string divisaAdquiere,
        decimal montoEntrega,
        decimal montoAdquiere,
        double ecRate,
        string fechaValor,
        string cuentaDebito,
        string cuentaCredito,
        string tipo)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.MUFG_MX2_LECTURA_MFX_INSERTAR_AUTORIZACION_FOREX_DESDE_TOMI,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdAutorizacion", idAutorizacion));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdCliente", idCliente));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@DivisaEntrega", divisaEntrega));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@DivisaAdquiere", divisaAdquiere));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@MontoEntrega", montoEntrega));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@MontoAdquiere", montoAdquiere));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@EcRate", ecRate));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValor));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaDebito", cuentaDebito));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaCredito", cuentaCredito));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Tipo", tipo));
            },
            validarAfectacion: false);

        return r.Exito;
    }


    /// <summary>
    /// Cancela autorización Forex en CMS.
    /// </summary>
    public async Task<bool> CancelarAutorizacionForex(long idAutorizacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.MUFG_MX2_LECTURA_MFX_CANCELAR_AUTORIZACION_FOREX,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@IdAutorizacion", idAutorizacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

}
using Aplicacion.DTOs.MFX;
using Aplicacion.Enums;
using Aplicacion.Interfaces.MFX;
using MUFG_T24.Comunes;
using MUFG_T24.Inicializacion_dtos;
using MUFG_T24.Repositorios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
/// <summary>
/// Representa MFXTomi Dao.
/// </summary>

public class MFXTomiDao : RepositorioMaestro, IMFXTomiDao
{
    /// <summary>
    /// Inicializa una nueva instancia de la clase MFXTomiDao.
    /// </summary>
    public MFXTomiDao()
        : base(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null) { }

    /// <summary>
    /// Obtiene la solicitud Forex por folio.
    /// </summary>
    public async Task<MFXSolicitudForexTomiDto> ObtenerSolicitudForexPorFolio(string folio)
    {
        var r = await EjecutarStoredProcedureQuery<MFXSolicitudForexTomiDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_OBTENER_SOLICITUD_FOREX_POR_FOLIO,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
            });

        return r.Data?.FirstOrDefault();
    }


    /// <summary>
    /// Actualiza los datos generales del maestro MFX.
    /// </summary>
    public async Task<bool> ActualizarMaestroDatosGral(
    string folio,
    string tipoOperacion,
    string fechaValorTexto,
    string monedaEntrega,
    decimal importeEntrega,
    long numeroCliente,
    string nombreCliente,
    string cuentaEntrega)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_ACTUALIZAR_MAESTRO_DATOS_GRAL,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaValor", fechaValorTexto));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@MonedaEntrega", monedaEntrega));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ImporteEntrega", importeEntrega));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NumeroCliente", numeroCliente));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NombreCliente", nombreCliente));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@CuentaEntrega", cuentaEntrega));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Cancela el maestro BTMUM desde estado LECTURA por Forex cancelado.
    /// </summary>
    public async Task<bool> CancelarMaestroPorForexCanceladoBTMUMDesdeLectura(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_BTMUM_DESDE_LECTURA,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Marca el maestro como ENVIO CMS.
    /// </summary>
    public async Task<bool> MarcarMaestroEnvioCms(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_MARCAR_MAESTRO_ENVIO_CMS,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Marca el maestro como ESPERANDO CONFIRMACION.
    /// </summary>
    public async Task<bool> MarcarMaestroEsperandoConfirmacion(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_MARCAR_MAESTRO_ESPERANDO_CONFIRMACION,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Cancela el maestro BTMUM desde ESPERANDO CONFIRMACION por Forex cancelado.
    /// </summary>
    public async Task<bool> CancelarMaestroPorForexCanceladoBTMUMDesdeEsperandoConfirmacion(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_BTMUM_DESDE_ESPERANDO_CONFIRMACION,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Marca el maestro como LECTURA con folio CMS asociado.
    /// </summary>
    public async Task<bool> MarcarMaestroLecturaConFolioCms(
        string folio,
        string tipoOperacion,
        long folioCms)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_MARCAR_MAESTRO_LECTURA_CON_FOLIO_CMS,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Cuenta registros TOMBIN MFX por folio CMS.
    /// </summary>
    public async Task<int> ContarTombinMfxPorFolio(long folioCms)
    {
        var r = await EjecutarStoredProcedureQuery<ConteoDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_CONTAR_TOMBIN_MFX_POR_FOLIO,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
            });

        return r.Data?.FirstOrDefault()?.NumRegs ?? 0;
    }


    /// <summary>
    /// Actualiza la validación de sistema de la solicitud Forex.
    /// </summary>
    public async Task<bool> ActualizarSolicitudForexValidacionSistema(long folioForexNormalizado)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_ACTUALIZAR_SOLICITUD_FOREX_VALIDACION_SISTEMA,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioForexNormalizado", folioForexNormalizado));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Marca el maestro como ESPERANDO AUT. TESORERIA.
    /// </summary>
    public async Task<bool> MarcarMaestroEsperandoAutorizacionTesoreria(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_MARCAR_MAESTRO_ESPERANDO_AUTORIZACION_TESORERIA,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }


    /// <summary>
    /// Cancela el maestro BTMUM desde tesorería por Forex cancelado.
    /// </summary>
    public async Task<bool> CancelarMaestroPorForexCanceladoBTMUMDesdeTesoreria(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_BTMUM_DESDE_TESORERIA,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Actualiza TOMBIN MFX como rechazado.
    /// </summary>
    public async Task<bool> ActualizarTombinMfxRechazado(long folioCms)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_ACTUALIZAR_TOMBIN_MFX_RECHAZADO,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Cancela el maestro CMS por Forex cancelado.
    /// </summary>
    public async Task<bool> CancelarMaestroPorForexCanceladoCMS(string folio, string tipoOperacion)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_CANCELAR_MAESTRO_POR_FOREX_CANCELADO_CMS,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@TipoOperacion", tipoOperacion));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Actualiza TOMBIN MFX como confirmado.
    /// </summary>
    public async Task<bool> ActualizarTombinMfxConfirmado(long folioCms, string fechaConfirma)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_ACTUALIZAR_TOMBIN_MFX_CONFIRMADO,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FechaConfirma", fechaConfirma));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Obtiene datos Forex para conciliación.
    /// </summary>
    public async Task<MFXForexConciliacionDto> ObtenerDatosForexParaConciliacion(long folioForexNormalizado)
    {
        var r = await EjecutarStoredProcedureQuery<MFXForexConciliacionDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_OBTENER_DATOS_FOREX_PARA_CONCILIACION,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioForexNormalizado", folioForexNormalizado));
            });

        return r.Data?.FirstOrDefault();
    }

    /// <summary>
    /// Obtiene datos TOMI para conciliación.
    /// </summary>
    public async Task<MFXTomiConciliacionDto> ObtenerDatosTomiParaConciliacion(long folioCms, bool forexCancelado)
    {
        var r = await EjecutarStoredProcedureQuery<MFXTomiConciliacionDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_OBTENER_DATOS_TOMI_PARA_CONCILIACION,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@FolioCms", folioCms));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@ForexCancelado", forexCancelado));
            });

        return r.Data?.FirstOrDefault();
    }

    /// <summary>
    /// Marca la solicitud Forex como procesada.
    /// </summary>
    public async Task<bool> MarcarSolicitudForexProcesada(string folio)
    {
        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_LECTURA_MFX_MARCAR_SOLICITUD_FOREX_PROCESADA,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@Folio", folio));
            },
            validarAfectacion: false);

        return r.Exito;
    }

    /// <summary>
    /// Procedimiento legacy.
    /// </summary>
    public async Task<bool> InsertarFormatoMfxDesdeCms(MFXFormatoTomiInsertDto datos)
    {
        var pSalida = basedatosConexion.CrearParametroSalida("@Salida", System.Data.DbType.Int16);
        pSalida.Direction = System.Data.ParameterDirection.Output;

        var r = await EjecutarStoredProcedureQuery<ResultadoVacioDto>(
            ConstantesServicio.ProcedimientosMFX.USP_TOMBIN_INSERT_FORMATO_MFX,
            cmd =>
            {
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO_UNICO", datos.NfFolioUnico, 25));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_TIPO", datos.NfTipo, 3));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FOLIO", datos.NfFolio));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CUSTOMER_CODE", datos.T24CustomerCode, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_ACCT_NO", datos.T24DebitAcctNo, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_CREDIT_ACCT_NO", datos.T24CreditAcctNo, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_ESTATUS", datos.NfEstatus, 4));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_CAPTURA", datos.NfUsuarioCaptura, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_USUARIO_AUTORIZA", datos.NfUsuarioAutoriza, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_CAPTURA", datos.NfFechaCaptura));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_AUTORIZA", datos.NfFechaAutoriza));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_DEBIT_AMOUNT", datos.T24DebitAmount));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@T24_SHORT_NAME", datos.T24ShortName, 100));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_AUTORIZACION", datos.NfAutorizacion, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_FECHA_EJECUCION", datos.NfFechaEjecucion, 15));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_DIVISA_ADQUIERE", datos.NfDivisaAdquiere, 3));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MONTO_ADQUIERE", datos.NfMontoAdquiere));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_EC_RATE", datos.NfEcRate, 100));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CUENTA_CREDITO", datos.NfCuentaCredito, 18));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_DIVISA_ENTREGA", datos.NfDivisaEntrega, 60));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_MONTO_ENTREGA", datos.NfMontoEntrega, 60));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_CUENTA_DEBITO", datos.NfCuentaDebito, 100));
                cmd.Parameters.Add(basedatosConexion.CrearParametro("@NF_DESCRIPCION", datos.NfDescripcion, 13));
                cmd.Parameters.Add(pSalida);
            });

        return r.Exito && Convert.ToInt16(pSalida.Value) == 0;
    }



}
using Aplicacion;
using Aplicacion.DAO.Operaciones.PaP_FXD;
using Aplicacion.Interfaces;
using Aplicacion.Interfaces.CNT;
using Aplicacion.Interfaces.ENT_COAS;
using Aplicacion.Interfaces.ENT_SPID;
using Aplicacion.Interfaces.ENT_TEF;
using Aplicacion.Interfaces.FirmaElectronica;
using Aplicacion.Interfaces.FXD;
using Aplicacion.Interfaces.FXDLocal;
using Aplicacion.Interfaces.FXI;
using Aplicacion.Interfaces.FXI_H2H;
using Aplicacion.Interfaces.FXIGPH;
using Aplicacion.Interfaces.FXILocal;
using Aplicacion.Interfaces.FXT_GPH;
using Aplicacion.Interfaces.FXT_Local;
using Aplicacion.Interfaces.MercadoDinero;
using Aplicacion.Interfaces.MFX;
using Aplicacion.Interfaces.PaP;
using Aplicacion.Interfaces.PaP_FXI;
using Aplicacion.Interfaces.PaP_FXT;
using Aplicacion.Interfaces.PaP_SPID;
using Aplicacion.Interfaces.SER;
using Aplicacion.Interfaces.SPEI;
using Aplicacion.Interfaces.SPID;
using Aplicacion.Interfaces.SPID_H2H;
using Aplicacion.Interfaces.TAX;
using Aplicacion.Interfaces.TEF;
using Aplicacion.Interfaces.TEF_H2H;
using Aplicacion.Interfaces.TMD;
using Aplicacion.Modelos;
using Aplicacion.Servicios.FirmaElectronica;
using Aplicacion.Servicios.Operaciones;
using Infraestructura;
using Infraestructura.Repositorio.CNT;
using Infraestructura.Repositorio.ENT_COAS;
using Infraestructura.Repositorio.ENT_SPID;
using Infraestructura.Repositorio.ENT_TEF;
using Infraestructura.Repositorio.FXD;
using Infraestructura.Repositorio.FXDLocal;
using Infraestructura.Repositorio.FXI;
using Infraestructura.Repositorio.FXIGPH;
using Infraestructura.Repositorio.FXILocal;
using Infraestructura.Repositorio.FXT;
using Infraestructura.Repositorio.FXT_GPH;
using Infraestructura.Repositorio.FXT_Local;
using Infraestructura.Repositorio.FXTH2H;
using Infraestructura.Repositorio.MercadoDinero;
using Infraestructura.Repositorio.PaP_FXT;
using Infraestructura.Repositorio.PaP_SPID;
using Infraestructura.Repositorio.SER;
using Infraestructura.Repositorio.SPEI;
using Infraestructura.Repositorio.SPID;
using Infraestructura.Repositorio.SPID_H2H;
using Infraestructura.Repositorio.TAX;
using Infraestructura.Repositorio.TEF;
using Infraestructura.Repositorio.TEF_H2H;
using Infraestructura.SOAP;
using MUFG_T24.Comunes;
using MUFG_T24.Comunes.Encriptacion;
using MUFG_T24.Configuracion_utilidades;
using MUFG_T24.ControladorServicios_Utilidades;
using MUFG_T24.Infraestructura.Logging;
using MUFG_T24.Inicializacion_utilidades;
using MUFG_T24.Interfaces_utilidades;
using MUFG_T24.Modelos;
using MUFG_T24.Repositorios;
using SimpleInjector;
using SimpleInjector.Lifestyles;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Lectura
{
    public partial class ServiceHost : WindowsTcpPaqueteServiceHostBase
    {
        private Container _containerDI;
        private Configuracion _configuracionBase;
        private ConfiguracionModelo _configuracionServicio;

        protected override string NombreServicioLog => NombreServicio.Lectura.ToString();

        public ServiceHost() { }

        protected override void Inicializacion(string[] args, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            _containerDI = new SimpleInjector.Container();
            _containerDI.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();
            _containerDI.Register<Alertas>(() => new Alertas(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, null, null), Lifestyle.Singleton);
            _containerDI.Register<IAlertas>(() => _containerDI.GetInstance<Alertas>(), Lifestyle.Singleton);
            _containerDI.Register<IControlServicio, ControlServicio>();
            _containerDI.Register<IParametrizacion, Parametrizacion>();
            _containerDI.Register<ConexionT24Mw>(() => new ConexionT24Mw(BasedatosProveedor.SqlServer, NombreBasedatos.TOMI, BasedatosProveedor.SqlServer, NombreBasedatos.T24_VW), Lifestyle.Transient);
            _containerDI.Register<Comunes>();

            //Objeto que almacena la configuración necesaria para el funcionamiento del servicio...
            _configuracionBase = new Configuracion();
            _configuracionServicio = new ConfiguracionModelo();

            //Inicializa la obtención de la configuración general y el logging...
            {
                ConfiguracionEncriptadaHelper.Inicializar();
                ServicioConfiguracionSettings servicioMainConfiguracion = ConfiguracionEncriptadaHelper.ObtenerServicio(NombreServicio.Lectura.ToString());
                _configuracionBase.NombreServicio = servicioMainConfiguracion.NombreServicio;
                _configuracionBase.PathLog = servicioMainConfiguracion.Log;
                _configuracionBase.TamanoMaximoArchivoLogKB = servicioMainConfiguracion.TamanoMaximoArchivoLogKB.ToString();
                _configuracionBase.ForzaEjecucionServicios = servicioMainConfiguracion.ForzaEjecucionServicios;
                _configuracionBase.LimiteConcurrencia = servicioMainConfiguracion.LimiteConcurrencia;

                Logging.Init(_configuracionBase.PathLog, servicioMainConfiguracion.NombreServicio, servicioMainConfiguracion.TamanoMaximoArchivoLogKB);
            }

            _containerDI.Register<Configuracion>(() => _configuracionBase.Clone(), Lifestyle.Scoped);
            _containerDI.Register<ConfiguracionModelo>(() => _configuracionServicio.Clone(), Lifestyle.Scoped);

            //Registrar los Componentes de los Servicios
            _containerDI.Register<IGeneralDao, GeneralDao>(Lifestyle.Transient);
            _containerDI.Register<IComunSPID, ComunSPID>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.FirmaElectronica.ComunSPID>(Lifestyle.Transient);
            _containerDI.Register<IComunSPIDDao, ComunSPIDDao>(Lifestyle.Transient);
            _containerDI.Register<IComunFXD, Aplicacion.Servicios.FirmaElectronica.ComunFXD>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXTLocal>(Lifestyle.Transient);
            _containerDI.Register<IFXT_LocalDao, FXT_LocalDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.SAT>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Interfaces.SAT.ISATCmsDao, Infraestructura.Repositorio.SAT.SATCmsDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Interfaces.SAT.ISATTomiDao, Infraestructura.Repositorio.SAT.SATTomiDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Interfaces.SAT.ISATLegacyProcessor, Infraestructura.Repositorio.SAT.SATLegacyProcessor>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.SPID>(Lifestyle.Transient);
            _containerDI.Register<ISPIDCmsDao, SPIDCmsDao>(Lifestyle.Transient);
            _containerDI.Register<ISPIDTomiDao, SPIDTomiDao>(Lifestyle.Transient);
            _containerDI.Register<ISpidVistasT24Dao, SpidVistasT24Dao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.SPIDLocal>(Lifestyle.Transient);
            _containerDI.Register<ISPIDLocalDao, SPIDLocalDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.TEFLocal>(Lifestyle.Transient);
            _containerDI.Register<ITEFLocalDao, Infraestructura.Repositorio.TEF_Local.TEFLocalDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.TMD>(Lifestyle.Transient);
            _containerDI.Register<ITMDCmsDao, Infraestructura.Repositorio.TMD.TMDCmsDao>(Lifestyle.Transient);
            _containerDI.Register<ITMDTomiDao, Infraestructura.Repositorio.TMD.TMDTomiDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXTGPH>(Lifestyle.Transient);
            _containerDI.Register<IFXT_GPHDao, FXT_GPHDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.SPIDGPH>(Lifestyle.Transient);
            _containerDI.Register<ISPIDGphDao, Infraestructura.Repositorio.SPID_GPH.SPIDGphDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.TEFGPH>(Lifestyle.Transient);
            _containerDI.Register<ITEFGphDao, Infraestructura.Repositorio.TEF_GPH.TEFGphDao>(Lifestyle.Transient);
            _containerDI.Register<Aplicacion.Servicios.Operaciones.TEF_H2H>(Lifestyle.Transient);
            _containerDI.Register<ITefH2hTomiDao, TefH2hTomiDao>(Lifestyle.Transient);

            _containerDI.Register<IT24CuentaWebService, T24CuentaWebService>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.CNT>(Lifestyle.Transient);
            _containerDI.Register<ICNTDao, CNTDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.ENT_COAS>(Lifestyle.Transient);
            _containerDI.Register<IENTCOASTomiDao, ENTCOASTomiDao>(Lifestyle.Transient);
            _containerDI.Register<IENTCOASSpeiDao, ENTCOASSpeiDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.ENT_TEF>(Lifestyle.Transient);
            _containerDI.Register<IENTTEFTefDao, ENTTEFTefDao>(Lifestyle.Transient);
            _containerDI.Register<IENTTEFTomiDao, ENTTEFTomiDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.ENTSPEI>(Lifestyle.Transient);
            _containerDI.Register<IEntSpeiDao, EntSpeiDao>(Lifestyle.Transient);
            _containerDI.Register<IEntSpeiSpeiDao, EntSpeiSpeiDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.ENTSPID>(Lifestyle.Transient);
            _containerDI.Register<IENTSPIDTomiDao, ENTSPIDTomiDao>(Lifestyle.Transient);
            _containerDI.Register<IENTSPIDSpidDao, ENTSPIDSpidDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXD>(Lifestyle.Transient);
            _containerDI.Register<IFXDDao, FXDDao>(Lifestyle.Transient);
            _containerDI.Register<IFXDCmsDao, FXDCmsDao>(Lifestyle.Transient);

            _containerDI.Register<FXDGPH>(Lifestyle.Transient);
            _containerDI.Register<IFXDGPHDao, FXDGPHDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXDH2H>(Lifestyle.Transient);
            _containerDI.Register<IFXDH2HDao, FXDH2HDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXD_Local>(Lifestyle.Transient);
            _containerDI.Register<IFXDLocalDao, FXDLocalDao>(Lifestyle.Transient);
            _containerDI.Register<IFXDLocalT24Dao, FXDLocalT24Dao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXI>(Lifestyle.Transient);
            _containerDI.Register<IFxiTomiDao, FxiTomiDao>(Lifestyle.Transient);
            _containerDI.Register<IFxiCmsDao, FxiCmsDao>(Lifestyle.Transient);
            _containerDI.Register<IFxiT24Dao, FxiT24Dao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXI_H2H>(Lifestyle.Transient);
            _containerDI.Register<IFXI_H2HDao, FXI_H2HDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXIGPH>(Lifestyle.Transient);
            _containerDI.Register<IFXI_GPHDao, FXI_GPHDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXILocal>(Lifestyle.Transient);
            _containerDI.Register<IFXI_LocalDao, FXI_LocalDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXT>(Lifestyle.Transient);
            _containerDI.Register<IFxtTomiDao, FxtTomiDao>(Lifestyle.Transient);
            _containerDI.Register<IFxtCmsDao, FxtCmsDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.FXTH2H>(Lifestyle.Transient);
            _containerDI.Register<IFXTH2HDao, FXTH2HDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.MercadoDinero>(Lifestyle.Transient);
            _containerDI.Register<IMercadoDineroTomiDao, MercadoDineroTomiDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.MFX>(Lifestyle.Transient);
            _containerDI.Register<IMFXTomiDao, MFXTomiDao>(Lifestyle.Transient);
            _containerDI.Register<IMFXCmsDao, MFXCmsDao>(Lifestyle.Transient);
            _containerDI.Register<IMFXVistasT24Dao, MFXVistasT24Dao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.NOMFXD>(Lifestyle.Transient);
            _containerDI.Register<INOMFXDTomiDao, NOMFXDTomiDao>(Lifestyle.Transient);
            _containerDI.Register<INOMFXDCmsDao, NOMFXDCmsDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.PaP_FXT>(Lifestyle.Transient);
            _containerDI.Register<IPaPFXTCmsDao, PaPFXTCmsDao>(Lifestyle.Transient);
            _containerDI.Register<IPaPFXTTomiDao, PaPFXTTomiDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.PaP_SPID>(Lifestyle.Transient);
            _containerDI.Register<IPaPSpidCmsDao, PaPSpidCmsDao>(Lifestyle.Transient);
            _containerDI.Register<IPaPSpidInsightDao, PaPSpidInsightDao>(Lifestyle.Transient);
            _containerDI.Register<IPaPSpidTomiDao, PaPSpidTomiDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.PaP_FXD>(Lifestyle.Transient);
            _containerDI.Register<IPaPFXDDao, PaPFXDDao>(Lifestyle.Transient);
            _containerDI.Register<IPaPFXDCmsDao, PaPFXDCmsDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.PaP_TEF>(Lifestyle.Transient);
            _containerDI.Register<IPaPTEFDao, PaPTEFDao>(Lifestyle.Transient);
            _containerDI.Register<IPaPTEFCmsDao, PaPTEFCmsDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.PaPFXI>(Lifestyle.Transient);
            _containerDI.Register<IPaPFXITomiDao, PaPFXITomiDao>(Lifestyle.Transient);
            _containerDI.Register<IPaPFXICmsDao, PaPFXICmsDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.SER>(Lifestyle.Transient);
            _containerDI.Register<ISERTomiDao, SERTomiDao>(Lifestyle.Transient);
            _containerDI.Register<ISERCmsDao, SERCmsDao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.SPID_H2H>(Lifestyle.Transient);
            _containerDI.Register<ISpidH2hTomiDao, SpidH2hTomiDao>(Lifestyle.Transient);
            _containerDI.Register<ISpidH2hVistasT24Dao, SpidH2hVistasT24Dao>(Lifestyle.Transient);

            _containerDI.Register<Aplicacion.Servicios.Operaciones.TAX>(Lifestyle.Transient);
            _containerDI.Register<ITAXCmsDao, TAXCmsDao>(Lifestyle.Transient);
            _containerDI.Register<ITAXTomiDao, TAXTomiDao>(Lifestyle.Transient);


            _containerDI.Register<Aplicacion.Servicios.Operaciones.TEF>(Lifestyle.Transient);
            _containerDI.Register<ITEFTomiDao, TEFTomiDao>(Lifestyle.Transient);
            _containerDI.Register<ITEFCmsDao, TEFCmsDao>(Lifestyle.Transient);

            _containerDI.Register<OperacionServicio>();

            _containerDI.Verify();
        }
        
        protected override async Task ParametrizacionesAsync(CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                //Obtener Proceso Actual
                MUFG_T24.Interfaces_utilidades.IParametrizacion _parametrizacion = _containerDI.GetInstance<MUFG_T24.Interfaces_utilidades.IParametrizacion>();
                _configuracionBase = await _parametrizacion.ObtenerProcesoActualValidadoAsync(_configuracionBase, ct).ConfigureAwait(false);

                //Validación explícita posterior para revisión de contrato...
                _parametrizacion.ValidarProcesoActual(_configuracionBase);
                int pid = _configuracionBase.Pid.Value;

                IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();
                await _controlServicio.MarcarPuertoEnUsoAsync(pid, _configuracionBase.NombreProceso, _configuracionBase.NombreDisplay, _configuracionBase.NombreServicio);

                _configuracionBase.PuertoTCP = await _parametrizacion.ObtenerPuertos(_configuracionBase.NombreServicio, pid);
                ConfigurarTcpListener(int.Parse(_configuracionBase.PuertoTCP), _configuracionBase.LimiteConcurrencia ?? 5);


                _configuracionServicio.StrNombreServicio = _configuracionBase.NombreServicio;
                _configuracionServicio.NombreServicio = _configuracionBase.NombreServicio;
                _configuracionServicio.TimerInterval = _configuracionBase.TimerInterval;
                _configuracionServicio.Pid = _configuracionBase.Pid;
                _configuracionServicio.StrPID = pid;
                _configuracionServicio.StrDisplayName = _configuracionBase.NombreDisplay;
                _configuracionServicio.FechaSistema = _configuracionBase.FechaSistema;
                _configuracionServicio.StrFechaSistema = _configuracionBase.FechaSistema;
                _configuracionServicio.NombreProceso = _configuracionBase.NombreProceso;
                _configuracionServicio.AliasProceso = "";
                _configuracionBase.AliasProceso = "";
            }
            catch (Exception ex)
            {
                Logging.Write($"ERROR|ParametrizacionesAsync|{ex}");
                throw;
            }
        }

        protected override async Task<RespuestaResumenDto> ProcesarPaqueteAsync(SolicitudPaqueteDto paquete, string[] operaciones, CancellationToken token)
        {
            using (AsyncScopedLifestyle.BeginScope(_containerDI))
            {
                var operacionServicio = _containerDI.GetInstance<OperacionServicio>();
                return await operacionServicio.EjecutaProcedimientosAsync(paquete.Tipo, operaciones, token).ConfigureAwait(false);
            }
        }

        protected override async Task TerminacionAsync(CancellationToken waitCt, CancellationToken cleanupCt, Task startupTask, Task runningTask)
        {
            Logging.Write("INFO|OnStop|Finalizando Servicio.");

            try
            {
                using (AsyncScopedLifestyle.BeginScope(_containerDI))
                {
                    IControlServicio _controlServicio = _containerDI.GetInstance<IControlServicio>();

                    await _controlServicio.EsperarTareaActivaAsync(startupTask, "ParametrizacionInicial", waitCt).ConfigureAwait(false);
                    await _controlServicio.EsperarTareaActivaAsync(runningTask, "ProcesamientoActivo", waitCt).ConfigureAwait(false);

                    await EsperarClientesActivosAsync(cleanupCt).ConfigureAwait(false);

                    Configuracion _configuracion = _containerDI.GetInstance<Configuracion>();

                    await _controlServicio.MarcarPuertoEnUsoAsync(null, null, null, _configuracion.NombreServicio, cleanupCt).ConfigureAwait(false);
                    await _controlServicio.ActualizaTerminacion(_configuracion.NombreServicio, cleanupCt).ConfigureAwait(false);
                }

                Logging.Write("INFO|OnStop|Terminación exitosa de bases de datos.");
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Logging.Write($"ERROR|TerminacionAsync|{ex}");
            }
        }

    }
}
namespace Lectura
{
    partial class ServiceHost
    {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            components = new System.ComponentModel.Container();
            this.ServiceName = "Service1";
        }

        #endregion
    }
}
using MUFG_T24.Comunes;
using MUFG_T24.ControladorServicios_Utilidades;

namespace Lectura
{
    /// <summary>
    /// Clase de entrada.
    /// </summary>
    internal static class Program
    {
        private const string NombreServicioInstalado = ConstantesServicios.NombreDisplay + ConstantesServicios.Lectura;

        /// <summary>
        /// Punto de entrada Main.
        /// Delega la ejecución al WindowsServiceProgramRunner para manejar la ejecución del servicio de Windows.
        /// </summary>
        private static void Main(string[] args)
        {
            WindowsServiceProgramRunner.Run(
                NombreServicioInstalado,
                () => new ServiceHost(),
                args,
                (service, startupArgs) => ((ServiceHost)service).DebugRun(startupArgs));
        }

    }
}

Lectura/app.config
<?xml version="1.0" encoding="utf-8" ?>
<configuration />

Lectura.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{2A3E13DD-9AF7-48CB-8975-B4211D3AB422}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <RootNamespace>Lectura</RootNamespace>
    <NOPASA>Lectura</NOPASA>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <PlatformTarget>x86</PlatformTarget>
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <PlatformTarget>x86</PlatformTarget>
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup>
    <StartupObject />
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
    <Prefer32Bit>true</Prefer32Bit>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
    <Prefer32Bit>true</Prefer32Bit>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="Newtonsoft.Json, Version=13.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed, processorArchitecture=MSIL">
      <HintPath>packages\Newtonsoft.Json.13.0.4\lib\net45\Newtonsoft.Json.dll</HintPath>
    </Reference>
    <Reference Include="SimpleInjector, Version=5.0.0.0, Culture=neutral, PublicKeyToken=984cb50dea722e99, processorArchitecture=MSIL">
      <HintPath>packages\SimpleInjector.5.5.0\lib\net461\SimpleInjector.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.ServiceProcess" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="ServiceHost.cs">
      <SubType>Component</SubType>
    </Compile>
    <Compile Include="ServiceHost.Designer.cs">
      <DependentUpon>ServiceHost.cs</DependentUpon>
    </Compile>
    <Compile Include="Program.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include=".github\copilot-instructions.md" />
    <None Include="App.config" />
    <None Include="docs\repo-structure-notes.md" />
    <None Include="packages.config" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\AplicacionLectura.csproj">
      <Project>{d354fd41-b160-474f-8702-704ac5dfe18f}</Project>
      <Name>AplicacionLectura</Name>
    </ProjectReference>
    <ProjectReference Include="..\Infraestructura\InfraestructuraLectura.csproj">
      <Project>{4c5cd0f6-2353-4048-8fc7-0881d9b778a1}</Project>
      <Name>InfraestructuraLectura</Name>
    </ProjectReference>
  </ItemGroup>
  <ItemGroup>
    <Content Include="Lectura\Srcripts\Operaciones\ENT_TEF.sql" />
    <Content Include="Lectura\Srcripts\Operaciones\MD.sql" />
    <Content Include="Lectura\Srcripts\Operaciones\PaP_FXI.sql" />
    <Content Include="Lectura\Srcripts\Operaciones\PaP_SPID.sql" />
    <Content Include="Lectura\Srcripts\Operaciones\PaP_TEF.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunFXD.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunFXD\GFD_ACTUALIZAR_FIRMA_DIGITAL.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunFXD\GFD_CONSULTAR_DATOS_FIRMA.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunFXD\usp_LecturaComunFXD_ActualizarFirmaDigital.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunFXD\usp_LecturaComunFXD_ObtenerDatosFirma.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunSPID.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunSPID\usp_LecturaSPID_ActualizarFirmaDigital.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunSPID\usp_LecturaSPID_ObtenerDatosFirma.sql" />
    <Content Include="Srcripts\FirmaElectronica\ComunSPID\usp_LecturaSPID_ObtenerDatosGeneracionFirma.sql" />
    <Content Include="Srcripts\Operaciones\CNT.sql" />
    <Content Include="Srcripts\Operaciones\CNT\usp_LecturaCNT_ActualizarDatosPlanTrabajoMaestroDesdeConcentradora.sql" />
    <Content Include="Srcripts\Operaciones\CNT\usp_LecturaCNT_ActualizarFolioUnicoConcentradora.sql" />
    <Content Include="Srcripts\Operaciones\CNT\usp_LecturaCNT_ContarEntradasSalidasPorReferencia.sql" />
    <Content Include="Srcripts\Operaciones\CNT\usp_LecturaCNT_ObtenerConcentradoraPorFolio.sql" />
    <Content Include="Srcripts\Operaciones\CNT\usp_LecturaCNT_ObtenerEntradaSalidaPorReferencia.sql" />
    <Content Include="Srcripts\Operaciones\ENT_COAS.sql" />
    <Content Include="Srcripts\Operaciones\ENT_SPEI.sql" />
    <Content Include="Srcripts\Operaciones\ENT_SPID.sql" />
    <Content Include="Srcripts\Operaciones\ENT_TEF.sql" />
    <Content Include="Srcripts\Operaciones\FXD.sql" />
    <Content Include="Srcripts\Operaciones\FXD_GPH.sql" />
    <Content Include="Srcripts\Operaciones\FXD_GPH\usp_LecturaFXDGPH_ActualizarFolioUnico.sql" />
    <Content Include="Srcripts\Operaciones\FXD_GPH\usp_LecturaFXDGPH_ActualizarPlanTrabajoMaestro.sql" />
    <Content Include="Srcripts\Operaciones\FXD_GPH\usp_LecturaFXDGPH_ObtenerDatosPorFolio.sql" />
    <Content Include="Srcripts\Operaciones\FXD_H2H.sql" />
    <Content Include="Srcripts\Operaciones\FXD_H2H\GFD_ACTUALIZAR_FIRMA_DIGITAL.sql" />
    <Content Include="Srcripts\Operaciones\FXD_H2H\GFD_CONSULTAR_DATOS_FIRMA.sql" />
    <Content Include="Srcripts\Operaciones\FXD_H2H\usp_LecturaFXDH2H_ActualizarPlanTrabajoMaestro.sql" />
    <Content Include="Srcripts\Operaciones\FXD_H2H\usp_LecturaFXDH2H_ObtenerDatosPorFolio.sql" />
    <Content Include="Srcripts\Operaciones\FXD_Local.sql" />
    <Content Include="Srcripts\Operaciones\FXI.sql" />
    <Content Include="Srcripts\Operaciones\FXI_GPH.sql" />
    <Content Include="Srcripts\Operaciones\FXI_H2H.sql" />
    <Content Include="Srcripts\Operaciones\FXI_H2H\usp_LecturaFXIH2H_ActualizarPlanTrabajoMaestroLectura.sql" />
    <Content Include="Srcripts\Operaciones\FXI_H2H\usp_LecturaFXIH2H_MarcarFechaFutura.sql" />
    <Content Include="Srcripts\Operaciones\FXI_H2H\usp_LecturaFXIH2H_ObtenerDatosBasicosPorFolio.sql" />
    <Content Include="Srcripts\Operaciones\FXI_Local.sql" />
    <Content Include="Srcripts\Operaciones\FXI_Local\usp_LecturaFXILocal_ActualizarFolioUnico.sql" />
    <Content Include="Srcripts\Operaciones\FXI_Local\usp_LecturaFXILocal_ActualizarPlanTrabajoMaestroLectura.sql" />
    <Content Include="Srcripts\Operaciones\FXI_Local\usp_LecturaFXILocal_ObtenerDatosBasicosPorFolio.sql" />
    <Content Include="Srcripts\Operaciones\FXT.sql" />
    <Content Include="Srcripts\Operaciones\FXT_GPH.sql" />
    <Content Include="Srcripts\Operaciones\FXT_H2H.sql" />
    <Content Include="Srcripts\Operaciones\FXT_Local.sql" />
    <Content Include="Srcripts\Operaciones\GENERAL\Renombrar_usp_LecturaENTSPID_MarcarPlanTrabajoConError.sql" />
    <Content Include="Srcripts\Operaciones\MD.sql" />
    <Content Include="Srcripts\Operaciones\MFX.sql" />
    <Content Include="Srcripts\Operaciones\NOM_FXD.sql" />
    <Content Include="Srcripts\Operaciones\PaP_FXD.sql" />
    <Content Include="Srcripts\Operaciones\PaP_FXI.sql" />
    <Content Include="Srcripts\Operaciones\PaP_FXT.sql" />
    <Content Include="Srcripts\Operaciones\PaP_SPID.sql" />
    <Content Include="Srcripts\Operaciones\PaP_TEF.sql" />
    <Content Include="Srcripts\Operaciones\SAT.sql" />
    <Content Include="Srcripts\Operaciones\SER.sql" />
    <Content Include="Srcripts\Operaciones\SPID.sql" />
    <Content Include="Srcripts\Operaciones\SPID_GPH.sql" />
    <Content Include="Srcripts\Operaciones\SPID_H2H.sql" />
    <Content Include="Srcripts\Operaciones\SPID_Local.sql" />
    <Content Include="Srcripts\Operaciones\TAX.sql" />
    <Content Include="Srcripts\Operaciones\TEF.sql" />
    <Content Include="Srcripts\Operaciones\TEF_GPH.sql" />
    <Content Include="Srcripts\Operaciones\TEF_H2H.sql" />
    <Content Include="Srcripts\Operaciones\TEF_Local.sql" />
    <Content Include="Srcripts\Operaciones\TMD.sql" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

AplicacionLectura.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{D354FD41-B160-474F-8702-704AC5DFE18F}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Aplicacion</RootNamespace>
    <NOPASA>AplicacionLectura</NOPASA>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.Runtime.Serialization" />
    <Reference Include="System.ServiceModel" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="DTOs\CNT\CntConcentradoraDto.cs" />
    <Compile Include="DTOs\CNT\CntEntradaSalidaDto.cs" />
    <Compile Include="DTOs\CNT\ConteoRegistrosDto.cs" />
    <Compile Include="DTOs\ComunSPID\ComunSPIDDtos.cs" />
    <Compile Include="DTOs\ConfiguracionServicio\OperacionDTO.cs" />
    <Compile Include="DTOs\ENT_COAS\ENT_COASDtos.cs" />
    <Compile Include="DTOs\ENT_SPEI\ConteoRegistrosDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiAutoDevolucionDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiCambioEstatusRespuestaDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiCuentaVostroDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiDataDevolucionDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiEntradaSalidaConciliacionDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiNfEntradaSpeiDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiOperacionRecibidaDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiParticipanteDto.cs" />
    <Compile Include="DTOs\ENT_SPEI\EntSpeiTomiConciliacionDto.cs" />
    <Compile Include="DTOs\ENT_SPID\ENT_SPIDDtos.cs" />
    <Compile Include="DTOs\ENT_TEF\ENT_TEFDtos.cs" />
    <Compile Include="DTOs\FirmaElectronica\ComunFXDFirmaDatosDto.cs" />
    <Compile Include="DTOs\FXD\ConteoRegistrosDto.cs" />
    <Compile Include="DTOs\FXD\ForexStatusDto.cs" />
    <Compile Include="DTOs\FXD\FormatoFxdCmsDto.cs" />
    <Compile Include="DTOs\FXD\FxdEntradaSalidaDto.cs" />
    <Compile Include="DTOs\FXD\FxdGphDatosDto.cs" />
    <Compile Include="DTOs\FXD\FxdH2hDatosDto.cs" />
    <Compile Include="DTOs\FXD\TombinFxdConciliacionDto.cs" />
    <Compile Include="DTOs\FXD\TombinFxdEntradaSalidaDto.cs" />
    <Compile Include="DTOs\FXD_Local\FXD_LocalDtos.cs" />
    <Compile Include="DTOs\FXI\FXIDtos.cs" />
    <Compile Include="DTOs\FXI_GPH\FXI_GPHDto.cs" />
    <Compile Include="DTOs\FXI_Local\FXI_LocalDtos.cs" />
    <Compile Include="DTOs\FXT\FXTDtos.cs" />
    <Compile Include="DTOs\FXT_GPH\FXT_GPHDto.cs" />
    <Compile Include="DTOs\FXT_H2H\FXT_H2HDtos.cs" />
    <Compile Include="DTOs\FXT_Local\FXT_LocalDto.cs" />
    <Compile Include="DTOs\MercadoDinero\MercadoDineroDtos.cs" />
    <Compile Include="DTOs\MFX\MFXDtos.cs" />
    <Compile Include="DTOs\NOM_FXD\NOM_FXDDtos.cs" />
    <Compile Include="DTOs\PaP_FXD\PaPFxdContenidoDto.cs" />
    <Compile Include="DTOs\PaP_FXD\ConteoRegistrosDto.cs" />
    <Compile Include="DTOs\PaP_FXD\ForexStatusDto.cs" />
    <Compile Include="DTOs\PaP_FXD\FxdEntradaSalidaDto.cs" />
    <Compile Include="DTOs\PaP_FXD\PaPFxdDetalleCmsDto.cs" />
    <Compile Include="DTOs\PaP_FXD\PapFxdDetalleImporteDto.cs" />
    <Compile Include="DTOs\PaP_FXD\PapFxdDetalleTomiDto.cs" />
    <Compile Include="DTOs\PaP_FXD\PaPFxdMaestroCmsDto.cs" />
    <Compile Include="DTOs\PaP_FXD\PapFxdSumatoriaDetalleDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTEFActualizarDatosGeneralesPlanTrabajoDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTEFCancelarPlanTrabajoForexDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTEFCancelarPlanTrabajoMaestroForexDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PapTefContenidoFormatoDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefConteoDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PapTefDetalleCmsDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefDetalleTomiConciliacionDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefDetalleTomiEntradaSalidaDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTEFEntradaSalidaConciliacionDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefForexStatusDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefInsertarDetalleTomiDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefInsertarEntradaSalidaTomiDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefInsertarMaestroTomiDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PapTefMaestroCmsDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTefResultadoSalidaDto.cs" />
    <Compile Include="DTOs\PaP_TEF\PaPTEFSumarizadoDetalleTomiDto.cs" />
    <Compile Include="DTOs\PaP_FXI\PaPFXIDtos.cs" />
    <Compile Include="DTOs\PaP_FXT\PaP_FXTDto.cs" />
    <Compile Include="DTOs\PaP_SPID\PaP_SPIDDtos.cs" />
    <Compile Include="DTOs\RespuestaErrorDto.cs" />
    <Compile Include="DTOs\RespuestaPaqueteDto.cs" />
    <Compile Include="DTOs\RespuestaResumenDto.cs" />
    <Compile Include="DTOs\RespuestaTiempoDto.cs" />
    <Compile Include="DTOs\SAT\SAT_DTO.cs" />
    <Compile Include="DTOs\SAT\SAT_Formato_DTO.cs" />
    <Compile Include="DTOs\SAT\SAT_HdClientes_DTO.cs" />
    <Compile Include="DTOs\SER\SERDtos.cs" />
    <Compile Include="DTOs\SolicitudOperacionDto.cs" />
    <Compile Include="DTOs\SolicitudPaqueteDto.cs" />
    <Compile Include="DTOs\SPID\SPIDLocalDtos.cs" />
    <Compile Include="DTOs\SPID\SPID_DiasNoHabiles_DTO.cs" />
    <Compile Include="DTOs\SPID\SPID_ForexBancosFax_DTO.cs" />
    <Compile Include="DTOs\SPID\SPID_Formato_DTO.cs" />
    <Compile Include="DTOs\SPID\SPID_Tombin_ES_DTO.cs" />
    <Compile Include="DTOs\SPID\SPID_Tombin_NF_DTO.cs" />
    <Compile Include="DTOs\SPID_GPH\SPIDGphDtos.cs" />
    <Compile Include="DTOs\SPID_H2H\SPID_H2HDtos.cs" />
    <Compile Include="DTOs\TAX\TAXDtos.cs" />
    <Compile Include="DTOs\TEF\TEFDtos.cs" />
    <Compile Include="DTOs\TEF_GPH\TEFGphDtos.cs" />
    <Compile Include="DTOs\TEF_H2H\TEF_H2HDtos.cs" />
    <Compile Include="DTOs\TEF_Local\TEFLocalDtos.cs" />
    <Compile Include="DTOs\TMD\TMD_Formato_DTO.cs" />
    <Compile Include="DTOs\TMD\TMD_NF_DTO.cs" />
    <Compile Include="DTOs\TMD\TMD_Solicitudes_DTO.cs" />
    <Compile Include="Enums\ConstantesServicio.cs" />
    <Compile Include="Interfaces\CNT\ICNTDao.cs" />
    <Compile Include="DTOs\SAT\CONTEO_DIAS_INHABIL_SAT_DTO.cs" />
    <Compile Include="DTOs\SAT\SALIDAAPPLET_UTILERIAS_SAT_DTO.cs" />
    <Compile Include="Enums\Enum.cs" />
    <Compile Include="Interfaces\ComunSPID\IComunSPIDDao.cs" />
    <Compile Include="Interfaces\ENT_COAS\IENTCOASSpeiDao.cs" />
    <Compile Include="Interfaces\ENT_COAS\IENTCOASTomiDao.cs" />
    <Compile Include="Interfaces\ENT_SPEI\IEntSpeiDao.cs" />
    <Compile Include="Interfaces\ENT_SPEI\IEntSpeiSpeiDao.cs" />
    <Compile Include="Interfaces\ENT_SPID\IENTSPIDSpidDao.cs" />
    <Compile Include="Interfaces\ENT_SPID\IENTSPIDTomiDao.cs" />
    <Compile Include="Interfaces\ENT_TEF\IENTTEFTefDao.cs" />
    <Compile Include="Interfaces\ENT_TEF\IENTTEFTomiDao.cs" />
    <Compile Include="Interfaces\FirmaElectronica\IComunFXD.cs" />
    <Compile Include="Interfaces\FirmaElectronica\IComunSPID.cs" />
    <Compile Include="Interfaces\FXD\IFXDCmsDao.cs" />
    <Compile Include="Interfaces\FXD\IFXDDao.cs" />
    <Compile Include="Interfaces\FXD\IFXDGPHDao.cs" />
    <Compile Include="Interfaces\FXD\IFXDH2HDao.cs" />
    <Compile Include="Interfaces\FXD_Local\IFXDLocalDao.cs" />
    <Compile Include="Interfaces\FXD_Local\IFXDLocalT24Dao.cs" />
    <Compile Include="Interfaces\FXI\IFxiT24Dao.cs" />
    <Compile Include="Interfaces\FXI\IFxiCmsDao.cs" />
    <Compile Include="Interfaces\FXI\IFxiTomiDao.cs" />
    <Compile Include="Interfaces\FXI_GPH\IFXI_GPHDao.cs" />
    <Compile Include="DTOs\FXI_H2H\FXI_H2HDtos.cs" />
    <Compile Include="Interfaces\FXI_H2H\IFXI_H2HDao.cs" />
    <Compile Include="Interfaces\FXI_Local\IFXI_LocalDao.cs" />
    <Compile Include="Interfaces\FXT\IFxtCmsDao.cs" />
    <Compile Include="Interfaces\FXT\IFxtTomiDao.cs" />
    <Compile Include="Interfaces\FXT_GPH\IFXT_GPHDao.cs" />
    <Compile Include="Interfaces\FXT_H2H\IFXTH2HDao.cs" />
    <Compile Include="Interfaces\FXT_Local\IFXT_LocalDao.cs" />
    <Compile Include="Interfaces\IGeneralDao.cs" />
    <Compile Include="Interfaces\IT24CuentaWebService.cs" />
    <Compile Include="Interfaces\MercadoDinero\IMercadoDineroTomiDao.cs" />
    <Compile Include="Interfaces\MFX\IMFXCmsDao.cs" />
    <Compile Include="Interfaces\MFX\IMFXTomiDao.cs" />
    <Compile Include="Interfaces\MFX\IMFXVistasT24Dao.cs" />
    <Compile Include="Interfaces\NOM_FXD\INOMFXDCmsDao.cs" />
    <Compile Include="Interfaces\NOM_FXD\INOMFXDTomiDao.cs" />
    <Compile Include="Interfaces\PaP\IPaPTEFCmsDao.cs" />
    <Compile Include="Interfaces\PaP\IPaPTEFDao.cs" />
    <Compile Include="Interfaces\PaP\IPaPFXDCmsDao.cs" />
    <Compile Include="Interfaces\PaP\IPaPFXDDao.cs" />
    <Compile Include="Interfaces\PaP_FXI\IPaPFXICmsDao.cs" />
    <Compile Include="Interfaces\PaP_FXI\IPaPFXITomiDao.cs" />
    <Compile Include="Interfaces\PaP_FXT\IPaPFXTCmsDao.cs" />
    <Compile Include="Interfaces\PaP_FXT\IPaPFXTTomiDao.cs" />
    <Compile Include="Interfaces\PaP_SPID\IPaPSpidCmsDao.cs" />
    <Compile Include="Interfaces\PaP_SPID\IPaPSpidInsightDao.cs" />
    <Compile Include="Interfaces\PaP_SPID\IPaPSpidTomiDao.cs" />
    <Compile Include="Interfaces\SAT\ISATCmsDao.cs" />
    <Compile Include="Interfaces\SAT\ISATLegacyProcessor.cs" />
    <Compile Include="Interfaces\SAT\ISATTomiDao.cs" />
    <Compile Include="Interfaces\SER\ISERCmsDao.cs" />
    <Compile Include="Interfaces\SER\ISERTomiDao.cs" />
    <Compile Include="Interfaces\SPID\ISPIDCmsDao.cs" />
    <Compile Include="Interfaces\SPID\ISPIDGphDao.cs" />
    <Compile Include="Interfaces\SPID\ISPIDLocalDao.cs" />
    <Compile Include="Interfaces\SPID\ISPIDTomiDao.cs" />
    <Compile Include="Interfaces\SPID\ISpidVistasT24Dao.cs" />
    <Compile Include="Interfaces\SPID_H2H\ISpidH2hTomiDao.cs" />
    <Compile Include="Interfaces\SPID_H2H\ISpidH2hVistasT24Dao.cs" />
    <Compile Include="Interfaces\TAX\ITAXCmsDao.cs" />
    <Compile Include="Interfaces\TAX\ITAXTomiDao.cs" />
    <Compile Include="Interfaces\TEF\ITEFCmsDao.cs" />
    <Compile Include="Interfaces\TEF\ITEFGphDao.cs" />
    <Compile Include="Interfaces\TEF\ITEFLocalDao.cs" />
    <Compile Include="Interfaces\TEF\ITEFTomiDao.cs" />
    <Compile Include="Interfaces\TEF_H2H\ITefH2hTomiDao.cs" />
    <Compile Include="Interfaces\TMD\ITMDCmsDao.cs" />
    <Compile Include="Interfaces\TMD\ITMDTomiDao.cs" />
    <Compile Include="Modelos\ComunFXD_GFDModelo.cs" />
    <Compile Include="Modelos\ConfiguracionModelo.cs" />
    <Compile Include="Servicios\FirmaElectronica\OrdenPagoByteArrayModelo.cs" />
    <Compile Include="Servicios\FirmaElectronica\OrdenPagoValueModelo.cs" />
    <Compile Include="Modelos\Result.cs" />
    <Compile Include="Modelos\SAT\SAT_Tombin_CMS_Modelo.cs" />
    <Compile Include="Modelos\SAT\SAT_ValoresFormato_Modelo.cs" />
    <Compile Include="Modelos\SpecialInternationalDto.cs" />
    <Compile Include="Modelos\SPID\SPID_InsertarEntradasSalidasModelo.cs" />
    <Compile Include="Modelos\SPID\SPID_InsertarFormatoModelo.cs" />
    <Compile Include="Modelos\TMD\TMD_InsertarFormatoModelo.cs" />
    <Compile Include="Modelos\TMD\TMD_Tombin_Modelo.cs" />
    <Compile Include="OperacionServicio.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Servicios\FirmaElectronica\ComunFXD.cs" />
    <Compile Include="Servicios\FirmaElectronica\ComunSPID.cs" />
    <Compile Include="Servicios\FirmaElectronica\MetodosFirma.cs" />
    <Compile Include="Servicios\Operaciones\CNT.cs" />
    <Compile Include="Servicios\Operaciones\ContenidoFormatoParser.cs" />
    <Compile Include="Servicios\Operaciones\ENTSPEI.cs" />
    <Compile Include="Servicios\Operaciones\ENT_COAS.cs" />
    <Compile Include="Servicios\Operaciones\ENTSPID.cs" />
    <Compile Include="Servicios\Operaciones\ENT_TEF.cs" />
    <Compile Include="Servicios\Operaciones\FXD.cs" />
    <Compile Include="Servicios\Operaciones\FXDGPH.cs" />
    <Compile Include="Servicios\Operaciones\FXDH2H.cs" />
    <Compile Include="Servicios\Operaciones\FXDLocal.cs" />
    <Compile Include="Servicios\Operaciones\FXI.cs" />
    <Compile Include="Servicios\Operaciones\FXIGPH.cs" />
    <Compile Include="Servicios\Operaciones\FXILocal.cs" />
    <Compile Include="Servicios\Operaciones\FXI_H2H.cs" />
    <Compile Include="Servicios\Operaciones\FXT.cs" />
    <Compile Include="Servicios\Operaciones\FXTGPH.cs" />
    <Compile Include="Servicios\Operaciones\FXTH2H.cs" />
    <Compile Include="Servicios\Operaciones\FXTLocal.cs" />
    <Compile Include="Servicios\Operaciones\MercadoDinero.cs" />
    <Compile Include="Servicios\Operaciones\MFX.cs" />
    <Compile Include="Servicios\Operaciones\NOMFXD.cs" />
    <Compile Include="Servicios\Operaciones\PaPFXI.cs" />
    <Compile Include="Servicios\Operaciones\PaP_FXD.cs" />
    <Compile Include="Servicios\Operaciones\PaP_FXT.cs" />
    <Compile Include="Servicios\Operaciones\PaP_SPID.cs" />
    <Compile Include="Servicios\Operaciones\PaP_TEF.cs" />
    <Compile Include="Servicios\Operaciones\SAT.cs" />
    <Compile Include="Servicios\Operaciones\SER.cs" />
    <Compile Include="Servicios\Operaciones\SPID.cs" />
    <Compile Include="Servicios\Operaciones\SPIDGPH.cs" />
    <Compile Include="Servicios\Operaciones\SPIDLocal.cs" />
    <Compile Include="Servicios\Operaciones\SPID_H2H.cs" />
    <Compile Include="Servicios\Operaciones\TAX.cs" />
    <Compile Include="Servicios\Operaciones\TEF.cs" />
    <Compile Include="Servicios\Operaciones\TEFGPH.cs" />
    <Compile Include="Servicios\Operaciones\TEFLocal.cs" />
    <Compile Include="Servicios\Operaciones\TEF_H2H.cs" />
    <Compile Include="Servicios\Operaciones\TMD.cs" />
  </ItemGroup>
  <ItemGroup>
    <WCFMetadata Include="Connected Services\" />
  </ItemGroup>
  <ItemGroup>
    <None Include="app.config" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>

InfraestructuraLectura.csproj
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="15.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{4C5CD0F6-2353-4048-8FC7-0881D9B778A1}</ProjectGuid>
    <OutputType>Library</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>Infraestructura</RootNamespace>
    <NOPASA>InfraestructuraLectura</NOPASA>
    <TargetFrameworkVersion>v4.8</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <Deterministic>true</Deterministic>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
    <PlatformTarget>x86</PlatformTarget>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Debug|x86'">
    <DebugSymbols>true</DebugSymbols>
    <OutputPath>bin\x86\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <DebugType>full</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)|$(Platform)' == 'Release|x86'">
    <OutputPath>bin\x86\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <Optimize>true</Optimize>
    <DebugType>pdbonly</DebugType>
    <PlatformTarget>x86</PlatformTarget>
    <LangVersion>7.3</LangVersion>
    <ErrorReport>prompt</ErrorReport>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="MUFG_T24, Version=1.0.0.0, Culture=neutral, PublicKeyToken=2eed977a6ac043c8, processorArchitecture=x86">
      <SpecificVersion>False</SpecificVersion>
      <HintPath>D:\ServiceCMS\MUFG_T24.dll</HintPath>
    </Reference>
    <Reference Include="System" />
    <Reference Include="System.Configuration" />
    <Reference Include="System.Core" />
    <Reference Include="System.Runtime.Serialization" />
    <Reference Include="System.ServiceModel" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Net.Http" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="Connected Services\wsInfoCuentaT24\Reference.cs">
      <AutoGen>True</AutoGen>
      <DesignTime>True</DesignTime>
      <DependentUpon>Reference.svcmap</DependentUpon>
    </Compile>
    <Compile Include="GeneralDao.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
    <Compile Include="Repositorio\CNT\CNTDao.cs" />
    <Compile Include="Repositorio\ComunSPID\ComunSPIDDao.cs" />
    <Compile Include="Repositorio\ENT_COAS\ENTCOASSpeiDao.cs" />
    <Compile Include="Repositorio\ENT_COAS\ENTCOASTomiDao.cs" />
    <Compile Include="Repositorio\ENT_SPEI\EntSpeiSpeiDao.cs" />
    <Compile Include="Repositorio\ENT_SPEI\EntSpeiDao.cs" />
    <Compile Include="Repositorio\ENT_SPID\ENTSPIDSpidDao.cs" />
    <Compile Include="Repositorio\ENT_SPID\ENTSPIDTomiDao.cs" />
    <Compile Include="Repositorio\ENT_TEF\ENTTEFTefDao.cs" />
    <Compile Include="Repositorio\ENT_TEF\ENTTEFTomiDao.cs" />
    <Compile Include="Repositorio\FXD\FXDCmsDao.cs" />
    <Compile Include="Repositorio\FXD\FXDDao.cs" />
    <Compile Include="Repositorio\FXD\FXDGPHDao.cs" />
    <Compile Include="Repositorio\FXD\FXDH2HDao.cs" />
    <Compile Include="Repositorio\FXD_Local\FXDLocalDao.cs" />
    <Compile Include="Repositorio\FXD_Local\FXDLocalT24Dao.cs" />
    <Compile Include="Repositorio\FXI\FXIT24Dao.cs" />
    <Compile Include="Repositorio\FXI\FXICmsDao.cs" />
    <Compile Include="Repositorio\FXI\FXIDao.cs" />
    <Compile Include="Repositorio\FXI_GPH\FXI_GPHDao.cs" />
    <Compile Include="Repositorio\FXI_H2H\FXI_H2HDao.cs" />
    <Compile Include="Repositorio\FXI_Local\FXI_LocalDao.cs" />
    <Compile Include="Repositorio\FXT\FXTCmsDao.cs" />
    <Compile Include="Repositorio\FXT\FXTTomiDao.cs" />
    <Compile Include="Repositorio\FXT_GPH\FXT_GPHDao.cs" />
    <Compile Include="Repositorio\FXT_H2H\FXTH2HDao.cs" />
    <Compile Include="Repositorio\FXT_Local\FXT_LocalDao.cs" />
    <Compile Include="Repositorio\MercadoDinero\MercadoDineroTomiDao.cs" />
    <Compile Include="Repositorio\MFX\MFXCmsDao.cs" />
    <Compile Include="Repositorio\MFX\MFXTomiDao.cs" />
    <Compile Include="Repositorio\MFX\MFXVistasT24Dao.cs" />
    <Compile Include="Repositorio\NOM_FXD\NOMFXDCmsDao.cs" />
    <Compile Include="Repositorio\NOM_FXD\NOMFXDTomiDao.cs" />
    <Compile Include="Repositorio\OutputParameterHelper.cs" />
    <Compile Include="Repositorio\PaP\PaPFXDCmsDao.cs" />
    <Compile Include="Repositorio\PaP\PaPFXDDao.cs" />
    <Compile Include="Repositorio\PaP\PaPTEFCmsDao.cs" />
    <Compile Include="Repositorio\PaP\PaPTEFDao.cs" />
    <Compile Include="Repositorio\PaP_FXI\PaPFXICmsDao.cs" />
    <Compile Include="Repositorio\PaP_FXI\PaPFXITomiDao.cs" />
    <Compile Include="Repositorio\PaP_FXT\PaPFXTCmsDao.cs" />
    <Compile Include="Repositorio\PaP_FXT\PaPFXTTomiDao.cs" />
    <Compile Include="Repositorio\PaP_SPID\PaP_SPIDCmsDao.cs" />
    <Compile Include="Repositorio\PaP_SPID\PaP_SPIDInsightDao.cs" />
    <Compile Include="Repositorio\PaP_SPID\PaP_SPIDTomiDao.cs" />
    <Compile Include="Repositorio\SAT\SATCmsDao.cs" />
    <Compile Include="Repositorio\SAT\SATLegacyProcessor.cs" />
    <Compile Include="Repositorio\SAT\SATTomiDao.cs" />
    <Compile Include="Repositorio\SAT\UtileriasSAT.cs" />
    <Compile Include="Repositorio\SER\SERCmsDao.cs" />
    <Compile Include="Repositorio\SER\SERTomiDao.cs" />
    <Compile Include="Repositorio\SPID\SPIDCmsDao.cs" />
    <Compile Include="Repositorio\SPID\SPIDLocalDao.cs" />
    <Compile Include="Repositorio\SPID\SPIDTomiDao.cs" />
    <Compile Include="Repositorio\SPID\SpidVistasT24Dao.cs" />
    <Compile Include="Repositorio\SPID_GPH\SPIDGphDao.cs" />
    <Compile Include="Repositorio\SPID_H2H\SPID_H2HDao.cs" />
    <Compile Include="Repositorio\SPID_H2H\SPID_H2HT24Dao.cs" />
    <Compile Include="Repositorio\TAX\TAXCmsDao.cs" />
    <Compile Include="Repositorio\TAX\TAXTomiDao.cs" />
    <Compile Include="Repositorio\TEF\TEFCmsDao.cs" />
    <Compile Include="Repositorio\TEF\TEFTomiDao.cs" />
    <Compile Include="Repositorio\TEF_GPH\TEFGphDao.cs" />
    <Compile Include="Repositorio\TEF_H2H\TEF_H2HDao.cs" />
    <Compile Include="Repositorio\TEF_Local\TEFLocalDao.cs" />
    <Compile Include="Repositorio\TMD\TMDCmsDao.cs" />
    <Compile Include="Repositorio\TMD\TMDTomiDao.cs" />
    <Compile Include="SOAP\T24CuentaWebService.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include="Connected Services\wsInfoCuentaT24\configuration91.svcinfo" />
    <None Include="Connected Services\wsInfoCuentaT24\configuration.svcinfo" />
    <None Include="Connected Services\wsInfoCuentaT24\Infraestructura.wsInfoCuentaT24.BTMACCOUNTSTATUSType.datasource">
      <DependentUpon>Reference.svcmap</DependentUpon>
    </None>
    <None Include="Connected Services\wsInfoCuentaT24\Infraestructura.wsInfoCuentaT24.Status.datasource">
      <DependentUpon>Reference.svcmap</DependentUpon>
    </None>
    <None Include="Connected Services\wsInfoCuentaT24\Infraestructura.wsInfoCuentaT24.VERIFICASTATUSDECUENTAResponse.datasource">
      <DependentUpon>Reference.svcmap</DependentUpon>
    </None>
    <None Include="Connected Services\wsInfoCuentaT24\Reference.svcmap">
      <Generator>WCF Proxy Generator</Generator>
      <LastGenOutput>Reference.cs</LastGenOutput>
    </None>
    <Content Include="Scripts\Lectura_StoredProcedures_CMS.sql" />
    <Content Include="Scripts\Lectura_StoredProcedures_TOMI.sql" />
    <Content Include="Scripts\Lectura_StoredProcedures_VistasT24.sql" />
  </ItemGroup>
  <ItemGroup>
    <None Include="app.config" />
    <None Include="Connected Services\wsInfoCuentaT24\services.xsd">
      <SubType>Designer</SubType>
    </None>
    <None Include="Connected Services\wsInfoCuentaT24\services1.xsd">
      <SubType>Designer</SubType>
    </None>
    <None Include="Connected Services\wsInfoCuentaT24\T24WebServicesImplService.wsdl" />
  </ItemGroup>
  <ItemGroup>
    <WCFMetadata Include="Connected Services\" />
  </ItemGroup>
  <ItemGroup>
    <WCFMetadataStorage Include="Connected Services\wsInfoCuentaT24\" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Aplicacion\AplicacionLectura.csproj">
      <Project>{D354FD41-B160-474F-8702-704AC5DFE18F}</Project>
      <Name>AplicacionLectura</Name>
    </ProjectReference>
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
</Project>


