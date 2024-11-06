import streamlit as st
from PIL import Image

# Page configuration
st.set_page_config(page_title="Our Team", page_icon="👥", layout="wide")

# Custom styling for text and layout
st.markdown("""
    <style>
    .title {
        font-size: 2.5rem;
        color: #FFA500;
        text-align: center;
        font-weight: bold;
        margin-bottom: 50px;
    }
    .member-name {
        font-size: 1.2rem;
        color: #333;
        font-weight: bold;
        text-align: center;
        margin-top: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# Title
st.markdown("<div class='title'>Meet Our Fantastic Team</div>", unsafe_allow_html=True)

# Team members - Update the image paths to point to each file
team_members = [
    {"name": "Alice Johnson", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"},
    {"name": "Bob Smith", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"},
    {"name": "Cathy Brown", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"},
    {"name": "David Wilson", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"},
    {"name": "Emma Davis", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"},
    {"name": "Frank Thomas", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"},
    {"name": "Grace Lee", "image_path": "/workspaces/test-multipage-web/pages/gojo.jpg"}
]

# Display team members in a grid with two rows and four columns
cols = st.columns(4)  # Creates 4 columns

for i, member in enumerate(team_members):
    col = cols[i % 4]  # Rotate through columns for each member
    with col:
        # Load the image
        image = Image.open(member["image_path"])
        
        # Display the image and name
        st.image(image, width=150, use_column_width="auto")
        st.markdown(f"<div class='member-name'>{member['name']}</div>", unsafe_allow_html=True)

# End
