#!/usr/bin/env python3
"""
Web interface for viewing and searching electrician data.
Run with: python web_app.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from flask import Flask, render_template, request, jsonify, send_file
from src.storage import DataStorage, ElectricianDB
from src.config import INDIAN_LOCATIONS
from sqlalchemy import func
from datetime import datetime
import json
import csv
import io

app = Flask(__name__, template_folder='templates', static_folder='static')
storage = DataStorage()


@app.route('/')
def index():
    """Main dashboard page."""
    stats = storage.get_statistics()
    
    # Get list of states and cities for dropdowns
    states = sorted(INDIAN_LOCATIONS.keys())
    
    return render_template('index.html', stats=stats, states=states)


@app.route('/api/states')
def get_states():
    """Get list of states with data."""
    session = storage.Session()
    try:
        states = session.query(
            ElectricianDB.state,
            func.count(ElectricianDB.id).label('count')
        ).group_by(ElectricianDB.state).order_by(ElectricianDB.state).all()
        
        return jsonify([{'state': s, 'count': c} for s, c in states])
    finally:
        session.close()


@app.route('/api/cities')
def get_all_cities():
    """Get all cities with data."""
    session = storage.Session()
    try:
        cities = session.query(
            ElectricianDB.city,
            ElectricianDB.state,
            func.count(ElectricianDB.id).label('count')
        ).group_by(ElectricianDB.city, ElectricianDB.state).order_by(ElectricianDB.city).all()
        
        return jsonify([{'city': c, 'state': s, 'count': cnt} for c, s, cnt in cities])
    finally:
        session.close()


@app.route('/api/cities/<state>')
def get_cities(state):
    """Get cities for a state."""
    session = storage.Session()
    try:
        cities = session.query(
            ElectricianDB.city,
            func.count(ElectricianDB.id).label('count')
        ).filter(ElectricianDB.state == state).group_by(ElectricianDB.city).order_by(ElectricianDB.city).all()
        
        return jsonify([{'city': c, 'count': cnt} for c, cnt in cities])
    finally:
        session.close()


@app.route('/api/search')
def search():
    """Search electricians with filters."""
    state = request.args.get('state', '')
    city = request.args.get('city', '')
    source = request.args.get('source', '')
    name = request.args.get('name', '')
    category = request.args.get('category', '')
    verified = request.args.get('verified', '')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    
    session = storage.Session()
    try:
        query = session.query(ElectricianDB)
        
        if state:
            query = query.filter(ElectricianDB.state.ilike(f'%{state}%'))
        if city:
            query = query.filter(ElectricianDB.city.ilike(f'%{city}%'))
        if source:
            query = query.filter(ElectricianDB.source.ilike(f'%{source}%'))
        if name:
            query = query.filter(ElectricianDB.name.ilike(f'%{name}%'))
        if category:
            query = query.filter(ElectricianDB.category.ilike(f'%{category}%'))
        if verified:
            query = query.filter(ElectricianDB.verified == (verified == '1'))
        
        total = query.count()
        
        records = query.order_by(ElectricianDB.state, ElectricianDB.city, ElectricianDB.name)\
            .offset((page - 1) * per_page)\
            .limit(per_page)\
            .all()
        
        results = []
        for r in records:
            results.append({
                'id': r.id,
                'name': r.name,
                'phone': r.phone,
                'city': r.city,
                'state': r.state,
                'address': r.address,
                'rating': r.rating,
                'review_count': r.review_count,
                'source': r.source,
                'source_url': r.source_url,
                'services': json.loads(r.services) if r.services else [],
                'experience_years': r.experience_years,
                'category': r.category,
                'service_description': r.service_description,
                'smart_meter_score': r.smart_meter_score,
                'smart_meter_notes': r.smart_meter_notes,
                'verified': r.verified,
                'verified_by': r.verified_by,
                'verified_at': r.verified_at.isoformat() if r.verified_at else None,
            })
        
        return jsonify({
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page,
            'results': results
        })
    finally:
        session.close()


@app.route('/api/stats')
def get_stats():
    """Get database statistics."""
    return jsonify(storage.get_statistics())


@app.route('/api/electrician/<int:id>', methods=['GET'])
def get_electrician(id):
    """Get single electrician details."""
    session = storage.Session()
    try:
        record = session.query(ElectricianDB).filter(ElectricianDB.id == id).first()
        if not record:
            return jsonify({'error': 'Not found'}), 404
        
        return jsonify({
            'id': record.id,
            'name': record.name,
            'phone': record.phone,
            'city': record.city,
            'state': record.state,
            'address': record.address,
            'rating': record.rating,
            'review_count': record.review_count,
            'source': record.source,
            'source_url': record.source_url,
            'services': json.loads(record.services) if record.services else [],
            'category': record.category,
            'service_description': record.service_description,
            'smart_meter_score': record.smart_meter_score,
            'smart_meter_notes': record.smart_meter_notes,
            'verified': record.verified,
            'verified_by': record.verified_by,
            'verified_at': record.verified_at.isoformat() if record.verified_at else None,
            'website': record.website,
        })
    finally:
        session.close()


@app.route('/api/electrician/<int:id>/verify', methods=['POST'])
def update_verification(id):
    """Update verification status."""
    session = storage.Session()
    try:
        record = session.query(ElectricianDB).filter(ElectricianDB.id == id).first()
        if not record:
            return jsonify({'error': 'Not found'}), 404
        
        data = request.get_json()
        
        record.verified = data.get('verified', False)
        record.verified_by = data.get('verified_by', 'User')
        record.verified_at = datetime.utcnow() if record.verified else None
        
        session.commit()
        
        return jsonify({
            'success': True,
            'verified': record.verified,
            'verified_by': record.verified_by,
            'verified_at': record.verified_at.isoformat() if record.verified_at else None,
        })
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@app.route('/api/electrician/<int:id>/update', methods=['POST'])
def update_electrician(id):
    """Update electrician details."""
    session = storage.Session()
    try:
        record = session.query(ElectricianDB).filter(ElectricianDB.id == id).first()
        if not record:
            return jsonify({'error': 'Not found'}), 404
        
        data = request.get_json()
        
        if 'category' in data:
            record.category = data['category']
        if 'service_description' in data:
            record.service_description = data['service_description']
        if 'smart_meter_score' in data:
            record.smart_meter_score = int(data['smart_meter_score'])
        if 'smart_meter_notes' in data:
            record.smart_meter_notes = data['smart_meter_notes']
        if 'verified' in data:
            record.verified = data['verified']
            record.verified_by = data.get('verified_by', 'User')
            record.verified_at = datetime.utcnow() if record.verified else None
        
        session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@app.route('/api/categories')
def get_categories():
    """Get list of categories."""
    session = storage.Session()
    try:
        categories = session.query(
            ElectricianDB.category,
            func.count(ElectricianDB.id).label('count')
        ).filter(ElectricianDB.category != None).group_by(ElectricianDB.category).all()
        
        return jsonify([{'category': c, 'count': cnt} for c, cnt in categories])
    finally:
        session.close()


@app.route('/api/electrician', methods=['POST'])
def create_electrician():
    """Create a new electrician record."""
    session = storage.Session()
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get('name') or not data.get('phone'):
            return jsonify({'error': 'Name and phone are required'}), 400
        
        # Check for duplicate phone
        existing = session.query(ElectricianDB).filter(
            ElectricianDB.phone == data.get('phone')
        ).first()
        
        if existing:
            return jsonify({'error': 'A record with this phone number already exists'}), 400
        
        record = ElectricianDB(
            name=data.get('name'),
            phone=data.get('phone'),
            city=data.get('city', ''),
            state=data.get('state', ''),
            address=data.get('address', ''),
            source='Manual Entry',
            source_url='',
            category=data.get('category', 'Electrician'),
            service_description=data.get('service_description', ''),
            smart_meter_score=int(data.get('smart_meter_score', 50)),
            smart_meter_notes=data.get('smart_meter_notes', ''),
            verified=data.get('verified', False),
            verified_by=data.get('verified_by', 'User') if data.get('verified') else None,
            verified_at=datetime.utcnow() if data.get('verified') else None,
            scraped_at=datetime.utcnow(),
            unique_key=f"manual_{data.get('name')}_{data.get('phone')}"
        )
        
        session.add(record)
        session.commit()
        
        return jsonify({
            'success': True,
            'id': record.id,
            'message': 'Record created successfully'
        })
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@app.route('/api/electrician/<int:id>', methods=['DELETE'])
def delete_electrician(id):
    """Delete an electrician record."""
    session = storage.Session()
    try:
        record = session.query(ElectricianDB).filter(ElectricianDB.id == id).first()
        if not record:
            return jsonify({'error': 'Not found'}), 404
        
        session.delete(record)
        session.commit()
        
        return jsonify({'success': True, 'message': 'Record deleted successfully'})
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@app.route('/api/export')
def export_data():
    """Export filtered data as CSV."""
    state = request.args.get('state', '')
    city = request.args.get('city', '')
    
    session = storage.Session()
    try:
        query = session.query(ElectricianDB)
        
        if state:
            query = query.filter(ElectricianDB.state.ilike(f'%{state}%'))
        if city:
            query = query.filter(ElectricianDB.city.ilike(f'%{city}%'))
        
        records = query.all()
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            'Name', 'Phone', 'City', 'State', 'Address', 
            'Rating', 'Reviews', 'Source', 'Services'
        ])
        
        # Data
        for r in records:
            services = json.loads(r.services) if r.services else []
            writer.writerow([
                r.name, r.phone, r.city, r.state, r.address,
                r.rating, r.review_count, r.source, '; '.join(services)
            ])
        
        output.seek(0)
        
        filename = f"electricians_{state or 'all'}_{city or 'all'}.csv"
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    finally:
        session.close()


if __name__ == '__main__':
    print("\n" + "="*50)
    print("🔌 India Electricians Database Viewer")
    print("="*50)
    print("\nStarting web server...")
    print("Open http://localhost:5000 in your browser\n")
    app.run(debug=True, host='0.0.0.0', port=5000)
