import requests
import json
import sys

def enviar_desafio(nombre, email, github_url):
    """
    Envía los datos del desafío mediante un POST request.
    
    Args:
        nombre (str): Nombre completo del participante
        email (str): Dirección de correo electrónico
        github_url (str): URL del repositorio de GitHub
        
    Returns:
        dict: Respuesta del servidor
    """
    
    # URL del endpoint
    url = "https://advana-challenge-check-api-cr-k4hdbggvoq-uc.a.run.app/data-engineer"
    
    # Preparar los datos
    datos = {
        "name": nombre,
        "mail": email,
        "github_url": github_url
    }
    
    try:
        # Realizar el POST request
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=datos, headers=headers)
        
        # Verificar si fue exitoso
        response.raise_for_status()
        
        # Retornar la respuesta como diccionario
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error al enviar el request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error al procesar la respuesta JSON: {e}")
        return None

def main():
    # Solicitar datos al usuario
    print("Por favor ingresa los siguientes datos:")
    nombre = 'JOSE ESTEBAN GALLEGOS ALIAGA'
    email = 'JGALLEGOSA@UNI.PE'
    github_url = 'https://github.com/josegallegosa/latam-challenge.git'
    
    # Enviar el desafío
    print("\nEnviando datos...")
    respuesta = enviar_desafio(nombre, email, github_url)
    
    # Mostrar resultado
    if respuesta:
        print("\nRespuesta del servidor:")
        print(json.dumps(respuesta, indent=2))
    else:
        print("\nNo se pudo completar el envío.")

if __name__ == "__main__":
    main()